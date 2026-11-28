mod workspace;

use anyhow::{Context, Result, bail, ensure};
use crossterm::{
    cursor::MoveTo,
    event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
    execute, queue,
    style::{Attribute, Print, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor},
    terminal::{self, Clear, ClearType, EnterAlternateScreen, LeaveAlternateScreen},
};
use nice_sync::{
    access::Membership,
    language::Applied,
    store::{read_json, write_json},
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::{self, File},
    io::{self, IsTerminal, Read, Seek, SeekFrom, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

#[derive(Clone, Default, Deserialize, Serialize)]
enum Config {
    #[default]
    Shared,
    Local(PathBuf),
    Publish(PathBuf),
    Applied,
}

impl Config {
    fn label(&self) -> String {
        match self {
            Self::Shared => "общий shared.sync".into(),
            Self::Local(path) => format!("локальный {}", path.display()),
            Self::Publish(path) => format!("общий {}", path.display()),
            Self::Applied => "применённый".into(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
struct Settings {
    config: Config,
    interval: u64,
    direct: Option<SocketAddr>,
    p2p_only: bool,
    show_route: bool,
    share: Option<String>,
    listen: Option<SocketAddr>,
    share_config: bool,
    editor: Option<String>,
    reduce_motion: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            config: Config::Shared,
            interval: 10,
            direct: None,
            p2p_only: false,
            show_route: false,
            share: None,
            listen: None,
            share_config: true,
            editor: None,
            reduce_motion: false,
        }
    }
}

impl Settings {
    fn sync(&self, command: &mut Command, repeat: bool) {
        command.arg(if repeat { "start" } else { "sync" });
        match &self.config {
            Config::Shared => {
                command.arg("--follow-config");
            }
            Config::Local(path) => {
                command.arg("--config").arg(path);
            }
            Config::Publish(path) => {
                command.arg("--publish-config").arg(path);
            }
            Config::Applied if repeat => {
                command.arg("--applied");
            }
            Config::Applied => {}
        }
        if repeat {
            command.arg("--every").arg(self.interval.to_string());
        }
        if let Some(address) = self.direct {
            command.arg("--direct").arg(address.to_string());
        }
        if self.p2p_only {
            command.arg("--p2p-only");
        }
        if self.show_route {
            command.arg("--show-route");
        }
        if let Some(share) = &self.share {
            command.arg("--share").arg(share);
        }
    }

    fn serve(&self, command: &mut Command) {
        command.arg("serve");
        if self.share_config {
            command.arg("--share-config");
            if let Some(editor) = &self.editor {
                command.arg("--config-editor").arg(editor);
            }
        }
        if let Some(address) = self.listen {
            command.arg("--direct").arg(address.to_string());
        }
    }
}

#[derive(Default)]
struct Session(Option<Child>);

impl Session {
    fn status(&mut self) -> Result<String> {
        match &mut self.0 {
            Some(child) => match child.try_wait()? {
                Some(status) => {
                    self.0 = None;
                    Ok(format!(
                        "Процесс завершён ({status}). Подробности в журнале."
                    ))
                }
                None => Ok(format!("Процесс запущен (PID {})", child.id())),
            },
            None => Ok("Обмен остановлен".into()),
        }
    }

    fn start(&mut self, command: &mut Command, log: &Path) -> Result<()> {
        ensure!(self.0.is_none(), "сначала остановите запущенный процесс");
        let output = File::create(log)?;
        self.0 = Some(
            command
                .stdin(Stdio::null())
                .stderr(output.try_clone()?)
                .stdout(output)
                .spawn()?,
        );
        Ok(())
    }

    fn stop(&mut self) -> Result<()> {
        if let Some(child) = &mut self.0 {
            if child.try_wait()?.is_none() && child.kill().is_err() && child.try_wait()?.is_none() {
                bail!("не удалось остановить процесс {}", child.id());
            }
            child.wait()?;
            self.0 = None;
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

pub fn run(state: Option<PathBuf>, home: Option<PathBuf>) -> Result<()> {
    ensure!(
        io::stdin().is_terminal() && io::stdout().is_terminal(),
        "the menu requires an interactive terminal; run nice-sync --help for commands"
    );
    let work = home.unwrap_or(env::current_dir()?);
    let state = std::path::absolute(state.unwrap_or_else(|| work.join(".nice-sync")))?;
    fs::create_dir_all(&state)?;
    let settings_file = state.join("terminal.json");
    let log = state.join("terminal.log");
    let mut settings: Settings = read_json(&settings_file)?;
    let executable = env::current_exe()?;
    let command = || {
        let mut command = Command::new(&executable);
        command.current_dir(&work).arg("--state").arg(&state);
        command
    };
    let mut session = Session::default();
    let mut cursor = 0;
    loop {
        let header = format!(
            "Nice Sync  |  {}\n{}  |  {}",
            state.display(),
            session.status()?,
            settings.config.label()
        );
        let choices = [
            "Запустить фоновый обмен",
            "Синхронизировать один раз",
            "Запустить публикацию папок",
            "Остановить процесс",
            "Настройки",
            "Редактировать конфиг",
            "Проверить и применить конфиг",
            "Подключиться по коду",
            "Журнал",
            "Обновить из GitHub",
            "Выход",
        ];
        let Some(action) = workspace::menu(
            &header,
            &choices,
            &mut cursor,
            &state,
            settings.reduce_motion,
        )?
        else {
            return session.stop();
        };
        if action == 10 {
            return session.stop();
        }
        let result = (|| -> Result<()> {
            let mut process = command();
            match action {
                0 => {
                    settings.sync(&mut process, true);
                    run_child(&mut process, "запуск фонового обмена")?;
                }
                1 => {
                    settings.sync(&mut process, false);
                    session.start(&mut process, &log)?;
                }
                2 => {
                    settings.serve(&mut process);
                    session.start(&mut process, &log)?;
                }
                3 => {
                    if session.0.is_some() {
                        session.stop()?;
                    } else {
                        crate::background::stop(&state)?;
                    }
                }
                4 => edit_settings(&mut settings, &settings_file, &work)?,
                5 => {
                    let file = config_file(&settings.config, &state, &work)?;
                    workspace::edit(&file, &state, settings.reduce_motion)?;
                }
                6 => {
                    let file = config_file(&settings.config, &state, &work)?;
                    if matches!(settings.config, Config::Shared | Config::Publish(_)) {
                        run_child(process.arg("check").arg(&file), "проверка")?;
                        println!("Общий конфиг применяется во время синхронизации.");
                    } else {
                        let device = Membership::load(&state)
                            .map(|member| member.device)
                            .or_else(|_| Applied::load(&state).map(|applied| applied.device))
                            .or_else(|_| prompt("Имя устройства"))?;
                        ensure!(!device.is_empty(), "укажите имя устройства");
                        run_child(
                            process.arg("apply").arg(&file).arg("--device").arg(device),
                            "применение",
                        )?;
                    }
                    pause()?;
                }
                7 => {
                    run_child(process.arg("join"), "подключение")?;
                    pause()?;
                }
                8 => {
                    let background_log = state.join("background/sync.log");
                    let selected = if session.0.is_some() || !background_log.is_file() {
                        &log
                    } else {
                        &background_log
                    };
                    let mut file =
                        File::open(selected).context("журнал появится после запуска обмена")?;
                    file.seek(SeekFrom::Start(file.metadata()?.len().saturating_sub(8192)))?;
                    let mut bytes = Vec::new();
                    file.take(8192).read_to_end(&mut bytes)?;
                    for character in String::from_utf8_lossy(&bytes).chars() {
                        if !character.is_control() || matches!(character, '\n' | '\r' | '\t') {
                            print!("{character}");
                        }
                    }
                    pause()?;
                }
                9 => {
                    run_child(process.arg("update"), "обновление")?;
                    println!("Следующий запуск использует новую сборку.");
                    pause()?;
                }
                _ => unreachable!(),
            }
            Ok(())
        })();
        if let Err(error) = result {
            eprintln!("{error:#}");
            pause()?;
        }
    }
}

fn config_file(config: &Config, state: &Path, work: &Path) -> Result<PathBuf> {
    match config {
        Config::Local(path) | Config::Publish(path) => Ok(path.clone()),
        Config::Applied => Ok(Applied::load(state)
            .ok()
            .and_then(|applied| applied.file)
            .unwrap_or_else(|| work.join("network.sync"))),
        Config::Shared => {
            let path = state.join("shared.sync");
            ensure!(
                path.is_file(),
                "сначала получите shared.sync при синхронизации"
            );
            Ok(path)
        }
    }
}

fn run_child(command: &mut Command, operation: &str) -> Result<()> {
    let status = command.status().with_context(|| operation.to_owned())?;
    ensure!(
        status.success(),
        "{operation} завершено с ошибкой ({status})"
    );
    Ok(())
}

fn edit_settings(saved: &mut Settings, file: &Path, work: &Path) -> Result<()> {
    let mut cursor = 0;
    loop {
        let mut settings = saved.clone();
        let value = |value: Option<String>| value.unwrap_or_else(|| "авто".into());
        let toggle = |value| if value { "да" } else { "нет" };
        let rows = [
            format!("Источник конфига  {}", settings.config.label()),
            format!("Интервал  {} с", settings.interval),
            format!(
                "Адрес подключения  {}",
                value(settings.direct.map(|a| a.to_string()))
            ),
            format!("Только P2P  {}", toggle(settings.p2p_only)),
            format!("Показывать маршрут  {}", toggle(settings.show_route)),
            format!(
                "Публикация для обмена  {}",
                settings.share.as_deref().unwrap_or("все")
            ),
            format!(
                "Адрес прослушивания  {}",
                value(settings.listen.map(|a| a.to_string()))
            ),
            format!("Раздавать конфиг  {}", toggle(settings.share_config)),
            format!(
                "Редактор общего конфига  {}",
                settings.editor.as_deref().unwrap_or("—")
            ),
            format!("Меньше анимации  {}", toggle(settings.reduce_motion)),
            "Назад".into(),
        ];
        let items = rows.each_ref().map(String::as_str);
        let result = (|| -> Result<bool> {
            match choose(
                "Настройки  |  изменения действуют при следующем запуске обмена",
                &items,
                &mut cursor,
            )? {
                Some(0) => {
                    let mut selected = 0;
                    let mode = choose(
                        "Источник конфига",
                        &[
                            "Общий shared.sync",
                            "Локальный файл",
                            "Общий файл редактора",
                            "Применённый",
                        ],
                        &mut selected,
                    )?;
                    settings.config = match mode {
                        Some(0) => Config::Shared,
                        Some(1 | 2) => {
                            let input = prompt("Путь к файлу")?;
                            ensure!(!input.is_empty(), "укажите путь к файлу");
                            let path = work.join(input);
                            if mode == Some(1) {
                                Config::Local(path)
                            } else {
                                Config::Publish(path)
                            }
                        }
                        Some(3) => Config::Applied,
                        _ => return Ok(true),
                    };
                }
                Some(1) => {
                    let interval = prompt("Интервал в секундах")?.parse()?;
                    ensure!(interval > 0, "интервал должен быть положительным");
                    settings.interval = interval;
                }
                Some(2) => {
                    settings.direct = optional(prompt("IP:порт, пусто для авто")?)
                        .map(|text| {
                            let address: SocketAddr = text.parse()?;
                            ensure!(
                                !address.ip().is_unspecified() && address.port() != 0,
                                "укажите адрес узла и ненулевой порт"
                            );
                            Ok(address)
                        })
                        .transpose()?
                }
                Some(3) => settings.p2p_only = !settings.p2p_only,
                Some(4) => settings.show_route = !settings.show_route,
                Some(5) => settings.share = optional(prompt("Имя публикации, пусто для всех")?),
                Some(6) => {
                    settings.listen = optional(prompt("IP:порт, пусто для авто")?)
                        .map(|text| text.parse())
                        .transpose()?
                }
                Some(7) => settings.share_config = !settings.share_config,
                Some(8) => {
                    settings.editor = optional(prompt("Имя устройства, пусто для отключения")?)
                }
                Some(9) => settings.reduce_motion = !settings.reduce_motion,
                _ => return Ok(false),
            }
            write_json(file, &settings)?;
            Ok(true)
        })();
        match result {
            Ok(false) => return Ok(()),
            Ok(true) => *saved = settings,
            Err(error) => {
                eprintln!("{error:#}");
                pause()?;
            }
        }
    }
}

fn optional(text: String) -> Option<String> {
    (!text.is_empty()).then_some(text)
}

fn prompt(label: &str) -> Result<String> {
    print!("{label}: ");
    io::stdout().flush()?;
    let mut value = String::new();
    ensure!(io::stdin().read_line(&mut value)? != 0, "ввод завершён");
    Ok(value.trim().to_owned())
}

fn pause() -> Result<()> {
    prompt("Enter — вернуться в меню")?;
    Ok(())
}

struct Screen;

impl Drop for Screen {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
        let _ = execute!(
            io::stdout(),
            ResetColor,
            SetAttribute(Attribute::Reset),
            LeaveAlternateScreen
        );
    }
}

fn choose(header: &str, rows: &[&str], selected: &mut usize) -> Result<Option<usize>> {
    terminal::enable_raw_mode()?;
    let _screen = Screen;
    let mut out = io::stdout();
    execute!(out, EnterAlternateScreen)?;
    loop {
        let (width, height) = terminal::size()?;
        let height = usize::from(height);
        let width = usize::from(width.saturating_sub(1));
        let heading: Vec<_> = header.lines().collect();
        let start = heading.len() + 1;
        let visible = height.saturating_sub(start + 1).max(1);
        let first = selected.saturating_sub(visible - 1);
        queue!(
            out,
            SetBackgroundColor(workspace::DARK),
            SetForegroundColor(workspace::TEXT),
            Clear(ClearType::All)
        )?;
        for (line, text) in heading.iter().enumerate().take(height) {
            queue!(
                out,
                MoveTo(0, line as u16),
                Print(
                    text.chars()
                        .filter(|c| !c.is_control())
                        .take(width)
                        .collect::<String>()
                )
            )?;
        }
        for (line, row) in rows.iter().skip(first).take(visible).enumerate() {
            let y = start.saturating_add(line).min(height.saturating_sub(1));
            let active = first + line == *selected;
            let text = format!("{} {row}", if active { "›" } else { " " });
            queue!(out, MoveTo(0, y as u16))?;
            if active {
                queue!(out, SetAttribute(Attribute::Reverse))?;
            }
            queue!(
                out,
                Print(
                    text.chars()
                        .filter(|c| !c.is_control())
                        .take(width)
                        .collect::<String>()
                ),
                SetAttribute(Attribute::Reset)
            )?;
        }
        if height > start + 1 {
            let keys: String = "↑ ↓  Выбор    Enter  Открыть    Esc  Назад"
                .chars()
                .take(width)
                .collect();
            queue!(out, MoveTo(0, (height - 1) as u16), Print(keys))?;
        }
        out.flush()?;
        if let Event::Key(key) = event::read()?
            && key.kind != KeyEventKind::Release
        {
            match key.code {
                KeyCode::Up => *selected = selected.checked_sub(1).unwrap_or(rows.len() - 1),
                KeyCode::Down => *selected = (*selected + 1) % rows.len(),
                KeyCode::Enter => return Ok(Some(*selected)),
                KeyCode::Esc => return Ok(None),
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    return Ok(None);
                }
                _ => {}
            }
        }
    }
}
