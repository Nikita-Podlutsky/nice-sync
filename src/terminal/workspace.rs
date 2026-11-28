use crate::{background, read_optional_text};
use anyhow::{Result, ensure};
use crossterm::{
    cursor::{Hide, MoveTo, Show},
    event::{
        self, DisableBracketedPaste, EnableBracketedPaste, Event, KeyCode, KeyEventKind,
        KeyModifiers,
    },
    execute, queue,
    style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use nice_sync::{language, store::write_text, sync::FolderProgress};
use std::{
    io::{self, Write},
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use unicode_width::UnicodeWidthChar;

pub(super) const TEXT: Color = Color::Rgb {
    r: 232,
    g: 237,
    b: 244,
};
const MUTED: Color = Color::Rgb {
    r: 162,
    g: 174,
    b: 191,
};
pub(super) const DARK: Color = Color::Rgb {
    r: 22,
    g: 25,
    b: 31,
};
const FOCUS: Color = Color::Rgb {
    r: 45,
    g: 55,
    b: 72,
};
const BLUE: Color = Color::Rgb {
    r: 109,
    g: 209,
    b: 233,
};
const PURPLE: Color = Color::Rgb {
    r: 202,
    g: 175,
    b: 251,
};
const AMBER: Color = Color::Rgb {
    r: 244,
    g: 197,
    b: 117,
};
const CORAL: Color = Color::Rgb {
    r: 255,
    g: 153,
    b: 148,
};

struct Screen;

impl Screen {
    fn enter() -> Result<Self> {
        terminal::enable_raw_mode()?;
        let screen = Self;
        execute!(
            io::stdout(),
            EnterAlternateScreen,
            EnableBracketedPaste,
            Hide
        )?;
        Ok(screen)
    }
}

impl Drop for Screen {
    fn drop(&mut self) {
        let _ = execute!(
            io::stdout(),
            ResetColor,
            DisableBracketedPaste,
            Show,
            LeaveAlternateScreen
        );
        let _ = terminal::disable_raw_mode();
    }
}

struct Frame {
    out: io::Stdout,
    width: u16,
    height: u16,
}

impl Frame {
    fn new() -> Result<Self> {
        let (width, height) = terminal::size()?;
        let mut out = io::stdout();
        queue!(out, Hide, SetBackgroundColor(DARK), Clear(ClearType::All))?;
        Ok(Self { out, width, height })
    }

    fn line(&mut self, x: u16, y: u16, width: u16, color: Color, text: &str) -> Result<()> {
        if x >= self.width || y >= self.height {
            return Ok(());
        }
        let mut cells = usize::from(width.min(self.width.saturating_sub(x + 1)));
        let text: String = text
            .chars()
            .filter(|ch| !ch.is_control())
            .take_while(|ch| {
                let size = ch.width().unwrap_or_default();
                if size > cells {
                    return false;
                }
                cells -= size;
                true
            })
            .collect();
        queue!(
            self.out,
            MoveTo(x, y),
            SetForegroundColor(color),
            Print(text)
        )?;
        Ok(())
    }

    fn footer(&mut self, text: &str) -> Result<()> {
        self.line(2, self.height.saturating_sub(1), self.width, MUTED, text)
    }
}

fn tick() -> Result<Option<Event>> {
    if event::poll(Duration::from_millis(300))? {
        return Ok(Some(event::read()?));
    }
    Ok(None)
}

pub(super) fn menu(
    header: &str,
    actions: &[&str],
    cursor: &mut usize,
    state: &Path,
    reduced: bool,
) -> Result<Option<usize>> {
    let _screen = Screen::enter()?;
    let mut pulse = 0;
    let mut folder = String::new();
    let mut search: Option<String> = None;
    loop {
        let mut frame = Frame::new()?;
        let wide = frame.width >= 80;
        frame.line(2, 0, frame.width, BLUE, "Nice Sync  /  Обзор")?;
        if let Some(context) = header.lines().nth(1) {
            frame.line(2, 1, frame.width, MUTED, context)?;
        }
        let status = background::snapshot(state)?;
        if !status.folders.contains_key(&folder)
            && let Some(first) = status.folders.keys().next()
        {
            folder.clone_from(first);
        }
        frame.transfer(
            if wide { 37 } else { 2 },
            if wide { 3 } else { 2 },
            &status,
            &folder,
            pulse,
            reduced,
            wide,
        )?;
        if wide {
            for y in 3..frame.height.saturating_sub(1) {
                frame.line(34, y, 1, FOCUS, "│")?;
            }
        }
        let filtered: Vec<usize> = actions
            .iter()
            .enumerate()
            .filter_map(|(index, action)| {
                search
                    .as_ref()
                    .is_none_or(|query| action.to_lowercase().contains(&query.to_lowercase()))
                    .then_some(index)
            })
            .collect();
        let start = if wide { 4 } else { 6 };
        let visible = frame.height.saturating_sub(start + 1).max(1) as usize;
        let active = filtered
            .iter()
            .position(|index| index == cursor)
            .unwrap_or_default();
        let first = active.saturating_sub(visible - 1);
        for (row, &index) in filtered.iter().skip(first).take(visible).enumerate() {
            let chosen = filtered.get(active) == Some(&index);
            queue!(
                frame.out,
                SetBackgroundColor(if chosen { FOCUS } else { DARK })
            )?;
            let marker = if chosen { "›" } else { " " };
            frame.line(
                2,
                start + row as u16,
                if wide { 31 } else { frame.width },
                if chosen { TEXT } else { MUTED },
                &format!("{marker} {}", actions[index]),
            )?;
        }
        queue!(frame.out, SetBackgroundColor(DARK))?;
        if let Some(query) = &search {
            frame.footer(&format!("/ {query}   Esc очистить"))?;
        } else {
            frame.footer("↑↓ Действие   Enter Открыть   Tab Папка   / Поиск   Esc Выход")?;
        }
        frame.out.flush()?;
        pulse = (pulse + 1) % 4;
        if let Some(Event::Key(key)) = tick()?
            && key.kind != KeyEventKind::Release
        {
            match key.code {
                KeyCode::Up if !filtered.is_empty() => {
                    *cursor = filtered[(active + filtered.len() - 1) % filtered.len()]
                }
                KeyCode::Down if !filtered.is_empty() => {
                    *cursor = filtered[(active + 1) % filtered.len()]
                }
                KeyCode::Enter => {
                    if let Some(&action) = filtered.get(active) {
                        *cursor = action;
                        return Ok(Some(action));
                    }
                }
                KeyCode::Tab if !status.folders.is_empty() => {
                    let mut names = status.folders.keys();
                    if let Some(next) = names
                        .find(|name| *name > &folder)
                        .or_else(|| status.folders.keys().next())
                    {
                        folder.clone_from(next);
                    }
                }
                KeyCode::Esc if search.is_some() => search = None,
                KeyCode::Esc => return Ok(None),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(None);
                }
                KeyCode::Char('/') if search.is_none() => search = Some(String::new()),
                KeyCode::Char(ch) => {
                    if let Some(query) = &mut search {
                        query.push(ch);
                    }
                }
                KeyCode::Backspace => {
                    if let Some(query) = &mut search {
                        query.pop();
                    }
                }
                _ => {}
            }
        }
    }
}

impl Frame {
    #[allow(clippy::too_many_arguments)]
    fn transfer(
        &mut self,
        x: u16,
        y: u16,
        status: &background::Progress,
        selected: &str,
        pulse: usize,
        reduced: bool,
        details: bool,
    ) -> Result<()> {
        let (phase, color) = match status.phase.as_str() {
            "syncing" => ("Обмен", BLUE),
            "stopping" => ("Завершается проход", AMBER),
            "waiting" if status.last_error.is_some() => ("Ожидание повтора", CORAL),
            "waiting" => ("Ожидание прохода", MUTED),
            "ready" | "starting" => ("Запуск", AMBER),
            _ => ("Фоновый обмен остановлен", MUTED),
        };
        let spinner = if status.phase == "syncing" && !reduced {
            ["·", "∙", "●", "∙"][pulse]
        } else {
            " "
        };
        let peer = if status.peer.is_empty() {
            String::new()
        } else {
            format!("  ·  {} ↔ {}", status.device, status.peer)
        };
        self.line(x, y, self.width, color, &format!("{spinner} {phase}{peer}"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let last = status
            .last_success_unix
            .map(|time| format!("Успешный обмен {} с назад", now.saturating_sub(time)))
            .unwrap_or_else(|| "Успешного обмена ещё не было".into());
        let retry = status
            .retry_unix
            .filter(|_| status.phase == "waiting")
            .map(|time| format!("  ·  далее через {} с", time.saturating_sub(now)))
            .unwrap_or_default();
        self.line(x, y + 1, self.width, MUTED, &format!("{last}{retry}"))?;
        if let Some(progress) = status.folders.get(selected) {
            self.folder(x, y + 2, selected, progress, details)?;
        }
        if details && let Some(error) = &status.last_error {
            self.line(
                x,
                y + 9,
                self.width,
                CORAL,
                error.lines().next().unwrap_or(error),
            )?;
        }
        if details {
            for (row, (name, folder)) in
                (y + 14..self.height.saturating_sub(1)).zip(&status.folders)
            {
                let (marker, color) = if folder.error.is_some() || folder.conflicts > 0 {
                    ("!", CORAL)
                } else if folder.phase == "complete" {
                    ("✓", MUTED)
                } else {
                    ("·", BLUE)
                };
                let focus = if name == selected { "›" } else { " " };
                self.line(
                    x,
                    row,
                    self.width,
                    color,
                    &format!("{focus} {marker} {name}"),
                )?;
            }
        }
        Ok(())
    }

    fn folder(
        &mut self,
        x: u16,
        y: u16,
        name: &str,
        folder: &FolderProgress,
        details: bool,
    ) -> Result<()> {
        let (label, color) = match (folder.phase.as_str(), folder.direction.as_deref()) {
            ("error", _) => ("Ошибка", CORAL),
            ("complete", _) if folder.conflicts > 0 => ("Есть конфликты", AMBER),
            ("complete", _) => ("Готово", MUTED),
            ("indexing", _) => ("Индексация", AMBER),
            ("comparing", _) => ("Сравнение", AMBER),
            (_, Some("upload")) => ("Отправка", PURPLE),
            (_, Some("download")) => ("Получение", BLUE),
            _ => ("Передача", MUTED),
        };
        self.line(x, y, self.width, color, &format!("{name}  ·  {label}"))?;
        if details {
            if let Some(total) = folder.total_bytes {
                let left = total.saturating_sub(folder.completed_bytes);
                let fill = if total == 0 {
                    0
                } else {
                    (folder.completed_bytes.min(total) as u128 * 18 / total as u128) as usize
                };
                self.line(
                    x,
                    y + 2,
                    self.width,
                    color,
                    &format!("{}{}", "━".repeat(fill), "─".repeat(18 - fill)),
                )?;
                self.line(
                    x,
                    y + 3,
                    self.width,
                    TEXT,
                    &format!(
                        "{:.2} / {:.2} МиБ  ·  осталось {:.2}",
                        mib(folder.completed_bytes),
                        mib(total),
                        mib(left)
                    ),
                )?;
                let eta = folder
                    .eta_seconds
                    .map(|eta| format!("  ·  ≈ {eta} с"))
                    .unwrap_or_default();
                self.line(
                    x,
                    y + 4,
                    self.width,
                    MUTED,
                    &format!(
                        "{}/{} файлов  ·  {:.2} МиБ/с{eta}",
                        folder.completed_files,
                        folder.total_files,
                        mib(folder.bytes_per_second)
                    ),
                )?;
            } else {
                self.line(
                    x,
                    y + 3,
                    self.width,
                    MUTED,
                    &format!("В индексе {} файлов", folder.indexed_files),
                )?;
            }
            for (index, path) in folder.upcoming.iter().take(3).enumerate() {
                self.line(
                    x,
                    y + 8 + index as u16,
                    self.width,
                    MUTED,
                    &format!("  {path}"),
                )?;
            }
            if let Some(path) = folder.error.as_ref().or(folder.path.as_ref()) {
                self.line(
                    x,
                    y + 5,
                    self.width,
                    if folder.error.is_some() { CORAL } else { TEXT },
                    path.lines().next().unwrap_or(path),
                )?;
            }
        }
        Ok(())
    }
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / 1_048_576.0
}

pub(super) fn edit(file: &Path, state: &Path, reduced: bool) -> Result<()> {
    let _screen = Screen::enter()?;
    let mut original = read_optional_text(file)?;
    let mut chars: Vec<char> = original
        .as_deref()
        .unwrap_or_default()
        .replace("\r\n", "\n")
        .chars()
        .collect();
    let mut cursor = 0;
    let mut pulse = 0;
    let mut dirty = false;
    let mut dismiss = false;
    let mut message = String::new();
    let mut saved = false;
    let mut undo = Vec::new();
    loop {
        let mut frame = Frame::new()?;
        let wide = frame.width >= 100;
        let side = if wide {
            frame.width * 3 / 5
        } else {
            frame.width
        };
        frame.line(
            2,
            0,
            frame.width,
            BLUE,
            &format!("Конфиг  /  {}", file.display()),
        )?;
        let status = background::snapshot(state)?;
        let selected = status.folders.keys().next().map_or("", String::as_str);
        frame.transfer(
            if wide { side + 2 } else { 2 },
            if wide { 2 } else { 1 },
            &status,
            selected,
            pulse,
            reduced,
            wide,
        )?;
        let footer = if dismiss {
            "Esc отбросить изменения   Любая клавиша — продолжить"
        } else {
            "Ctrl+S Проверить и сохранить   Ctrl+Z Отменить   Esc Назад"
        };
        frame.footer(footer)?;
        let top = 4;
        let body = usize::from(frame.height.saturating_sub(top + 2)).max(1);
        let row = chars[..cursor].iter().filter(|&&ch| ch == '\n').count();
        let head = chars[..cursor]
            .iter()
            .rposition(|&ch| ch == '\n')
            .map_or(0, |pos| pos + 1);
        let column: usize = chars[head..cursor].iter().map(|ch| cell_width(*ch)).sum();
        let horizontal = column.saturating_sub(usize::from(side.saturating_sub(9)));
        let first = row.saturating_sub(body - 1);
        for (index, text) in chars
            .split(|&ch| ch == '\n')
            .enumerate()
            .skip(first)
            .take(body)
        {
            let y = top + (index - first) as u16;
            frame.line(1, y, 5, MUTED, &format!("{:>4}", index + 1))?;
            let mut cells = 0;
            let text: String = text
                .iter()
                .copied()
                .skip_while(|ch| {
                    let skip = cells < horizontal;
                    cells += cell_width(*ch);
                    skip
                })
                .collect();
            let color = if text.trim_start().starts_with('#') {
                MUTED
            } else if text.contains('{') || text.contains('}') {
                PURPLE
            } else {
                TEXT
            };
            frame.line(6, y, side.saturating_sub(7), color, &text)?;
        }
        if frame.height > 2 {
            frame.line(
                2,
                frame.height - 2,
                frame.width,
                if message.is_empty() { MUTED } else { AMBER },
                if !message.is_empty() {
                    &message
                } else if dirty {
                    "Черновик"
                } else {
                    if original.as_deref().map(crate::config_digest).as_ref()
                        == status.config_hash.as_ref()
                        && status.config_hash.is_some()
                    {
                        "Конфиг используется в обмене"
                    } else {
                        if saved {
                            "Проверен и сохранён локально"
                        } else {
                            "Файл на диске"
                        }
                    }
                },
            )?;
        }
        if side > 9 && top + ((row - first) as u16) < frame.height.saturating_sub(2) {
            queue!(
                frame.out,
                MoveTo(6 + (column - horizontal) as u16, top + (row - first) as u16),
                Show
            )?;
        }
        frame.out.flush()?;
        pulse = (pulse + 1) % 4;
        let Some(event) = tick()? else { continue };
        if let Event::Paste(text) = event {
            remember(&mut undo, &chars, cursor);
            let inserted: Vec<char> = text
                .replace("\r\n", "\n")
                .chars()
                .filter(|&ch| !ch.is_control() || ch == '\n' || ch == '\t')
                .collect();
            let count = inserted.len();
            chars.splice(cursor..cursor, inserted);
            cursor += count;
            dirty = true;
            dismiss = false;
            message.clear();
            continue;
        }
        let Event::Key(key) = event else { continue };
        if key.kind == KeyEventKind::Release {
            continue;
        }
        if key.code == KeyCode::Esc {
            if !dirty || dismiss {
                return Ok(());
            }
            dismiss = true;
            continue;
        }
        dismiss = false;
        let ctrl = key.modifiers.contains(KeyModifiers::CONTROL);
        let end = chars[cursor..]
            .iter()
            .position(|&ch| ch == '\n')
            .map_or(chars.len(), |pos| cursor + pos);
        match key.code {
            KeyCode::Char('s') if ctrl => {
                let mut text: String = chars.iter().collect();
                if original
                    .as_ref()
                    .is_some_and(|value| value.contains("\r\n"))
                {
                    text = text.replace('\n', "\r\n");
                }
                if let Err(error) = language::parse(&text) {
                    message = format!("{error:#}");
                } else if read_optional_text(file)? != original {
                    let (mut draft, path) = tempfile::Builder::new()
                        .prefix("config-draft-")
                        .suffix(".sync")
                        .tempfile_in(state)?
                        .keep()?;
                    draft.write_all(text.as_bytes())?;
                    draft.sync_all()?;
                    message = format!("Файл обновился. Черновик: {}", path.display());
                } else {
                    if let Some(parent) = file.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    ensure!(
                        read_optional_text(file)? == original,
                        "configuration changed during save"
                    );
                    write_text(file, &text)?;
                    original = Some(text);
                    dirty = false;
                    saved = true;
                    message.clear();
                }
            }
            KeyCode::Char('z') if ctrl => {
                if let Some((old, position)) = undo.pop() {
                    chars = old;
                    cursor = position;
                    dirty = true;
                }
            }
            KeyCode::Home => cursor = head,
            KeyCode::End => cursor = end,
            KeyCode::Left => cursor = cursor.saturating_sub(1),
            KeyCode::Right => cursor = (cursor + 1).min(chars.len()),
            KeyCode::Up if head > 0 => {
                let previous = chars[..head - 1]
                    .iter()
                    .rposition(|&ch| ch == '\n')
                    .map_or(0, |pos| pos + 1);
                cursor = (previous + cursor - head).min(head - 1);
            }
            KeyCode::Down if end < chars.len() => {
                let next = chars[end + 1..]
                    .iter()
                    .position(|&ch| ch == '\n')
                    .map_or(chars.len(), |pos| end + 1 + pos);
                cursor = (end + 1 + cursor - head).min(next);
            }
            KeyCode::Backspace if cursor > 0 => {
                remember(&mut undo, &chars, cursor);
                cursor -= 1;
                chars.remove(cursor);
                dirty = true;
                message.clear();
            }
            KeyCode::Delete if cursor < chars.len() => {
                remember(&mut undo, &chars, cursor);
                chars.remove(cursor);
                dirty = true;
                message.clear();
            }
            KeyCode::Char(_) | KeyCode::Enter | KeyCode::Tab
                if !ctrl && !key.modifiers.contains(KeyModifiers::ALT) =>
            {
                let input = match key.code {
                    KeyCode::Char(ch) => ch,
                    KeyCode::Tab => '\t',
                    _ => '\n',
                };
                remember(&mut undo, &chars, cursor);
                chars.insert(cursor, input);
                cursor += 1;
                dirty = true;
                message.clear();
            }
            _ => {}
        }
    }
}

fn remember(undo: &mut Vec<(Vec<char>, usize)>, chars: &[char], cursor: usize) {
    if undo.len() == 20 {
        undo.remove(0);
    }
    undo.push((chars.to_vec(), cursor));
}

fn cell_width(ch: char) -> usize {
    if ch == '\t' {
        4
    } else {
        ch.width().unwrap_or_default()
    }
}
