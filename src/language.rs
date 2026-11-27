use crate::store::{read_json, write_json};
use anyhow::{Context, Result, ensure};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt, fs,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Right {
    Browse,
    Download,
    Archive,
    Receive,
    Checkout,
    Mirror,
}

impl Right {
    fn parse(word: &str) -> Option<Self> {
        Some(match word {
            "browse" => Self::Browse,
            "download" => Self::Download,
            "archive" => Self::Archive,
            "receive" => Self::Receive,
            "checkout" => Self::Checkout,
            "mirror" => Self::Mirror,
            _ => return None,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mode {
    Archive,
    Receive,
    Checkout,
    Mirror,
}

impl Mode {
    pub fn right(self) -> Right {
        match self {
            Self::Archive => Right::Archive,
            Self::Receive => Right::Receive,
            Self::Checkout => Right::Checkout,
            Self::Mirror => Right::Mirror,
        }
    }

    pub fn uploads(self) -> bool {
        self != Self::Checkout
    }

    pub fn downloads(self) -> bool {
        matches!(self, Self::Checkout | Self::Mirror)
    }
}

#[derive(Clone, Debug)]
pub struct Rule {
    pub mode: Mode,
    pub path: PathBuf,
    pub propagate_delete: bool,
    pub filters: Vec<Filter>,
}

#[derive(Clone, Debug)]
pub struct Share {
    pub host: String,
    pub path: PathBuf,
    pub filters: Vec<Filter>,
    pub rules: BTreeMap<String, Rule>,
}

pub type Grants = BTreeMap<String, BTreeSet<Right>>;

#[derive(Debug)]
pub struct Invitation {
    pub issuers: BTreeSet<String>,
    pub grants: Grants,
    pub expires: u64,
}

#[derive(Debug)]
pub struct Network {
    pub name: String,
    pub host: String,
    pub devices: BTreeSet<String>,
    pub shares: BTreeMap<String, Share>,
    pub filters: Vec<Filter>,
    pub permits: BTreeMap<String, Grants>,
    pub invitations: BTreeMap<String, Invitation>,
}

impl Network {
    pub fn ignored(&self, share: &Share, rule: Option<&Rule>, path: &str, directory: bool) -> bool {
        self.filters
            .iter()
            .chain(&share.filters)
            .chain(rule.into_iter().flat_map(|rule| &rule.filters))
            .any(|filter| filter.matches(path, directory))
    }
}

#[derive(Clone, Debug)]
pub struct Filter {
    directory: bool,
    pattern: Regex,
    source: String,
}

impl Filter {
    fn matches(&self, path: &str, directory: bool) -> bool {
        if self.directory {
            return directory && self.pattern.is_match(path)
                || path
                    .match_indices('/')
                    .any(|(end, _)| self.pattern.is_match(&path[..end]));
        }
        !directory && self.pattern.is_match(path)
    }
}

#[derive(Clone, Copy, Debug)]
struct Position {
    line: usize,
    column: usize,
}

#[derive(Debug)]
struct Diagnostic {
    category: &'static str,
    at: Position,
    message: String,
}

impl fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{}: {}: {}",
            self.at.line, self.at.column, self.category, self.message
        )
    }
}

impl std::error::Error for Diagnostic {}

impl Position {
    fn error(self, category: &'static str, message: impl Into<String>) -> anyhow::Error {
        Diagnostic {
            category,
            at: self,
            message: message.into(),
        }
        .into()
    }

    fn syntax(self, message: impl Into<String>) -> anyhow::Error {
        self.error("syntax", message)
    }

    fn semantic(self, message: impl Into<String>) -> anyhow::Error {
        self.error("semantic", message)
    }
}

#[derive(Clone, Debug)]
enum Token {
    Word(String),
    Text(String),
    Symbol(char),
    End,
}

fn lex(source: &str) -> Result<Vec<(Position, Token)>> {
    let mut tokens = Vec::new();
    let mut end = Position { line: 1, column: 1 };
    for (line_index, line) in source.split('\n').enumerate() {
        let mut chars = line.char_indices().peekable();
        let mut column = 0;
        while let Some((start, c)) = chars.next() {
            column += 1;
            let at = Position {
                line: line_index + 1,
                column,
            };
            let token = match c {
                c if c.is_whitespace() => continue,
                '#' => break,
                '{' | '}' | ',' | '*' => Token::Symbol(c),
                '"' => {
                    let mut closed = None;
                    let mut escaped = false;
                    for (index, c) in chars.by_ref() {
                        column += 1;
                        if c == '"' && !escaped {
                            closed = Some(index);
                            break;
                        }
                        escaped = c == '\\' && !escaped;
                    }
                    let finish = closed.ok_or_else(|| at.syntax("unterminated string"))?;
                    let value = serde_json::from_str(&line[start..=finish])
                        .map_err(|error| at.syntax(error.to_string()))?;
                    Token::Text(value)
                }
                c if c.is_ascii_alphanumeric() || c == '_' || c == '-' => {
                    while chars
                        .next_if(|(_, c)| c.is_ascii_alphanumeric() || matches!(c, '_' | '-'))
                        .is_some()
                    {
                        column += 1;
                    }
                    let finish = chars.peek().map_or(line.len(), |(index, _)| *index);
                    Token::Word(line[start..finish].to_owned())
                }
                _ => return Err(at.syntax(format!("unexpected character {c:?}"))),
            };
            tokens.push((at, token));
        }
        end = Position {
            line: line_index + 1,
            column: column + 1,
        };
    }
    tokens.push((end, Token::End));
    Ok(tokens)
}

type Name = (Position, String);

struct Parser {
    tokens: std::iter::Peekable<std::vec::IntoIter<(Position, Token)>>,
}

impl Parser {
    fn next(&mut self) -> (Position, Token) {
        let (at, token) = self.tokens.peek().cloned().expect("end token");
        if !matches!(token, Token::End) {
            self.tokens.next();
        }
        (at, token)
    }

    fn at(&mut self) -> Position {
        self.tokens.peek().expect("end token").0
    }

    fn eat(&mut self, symbol: char) -> bool {
        self.tokens
            .next_if(|(_, token)| matches!(token, Token::Symbol(c) if *c == symbol))
            .is_some()
    }

    fn symbol(&mut self, symbol: char) -> Result<()> {
        if !self.eat(symbol) {
            return Err(self.at().syntax(format!("expected '{symbol}'")));
        }
        Ok(())
    }

    fn word(&mut self) -> Result<Name> {
        let (at, token) = self.next();
        match token {
            Token::Word(word) => Ok((at, word)),
            _ => Err(at.syntax("expected a word")),
        }
    }

    fn name(&mut self) -> Result<Name> {
        let (at, name) = self.word()?;
        if !name.as_bytes()[0].is_ascii_lowercase()
            || name.len() > 63
            || name.bytes().any(|c| c.is_ascii_uppercase())
        {
            return Err(at.syntax(
                "expected a lowercase name starting with a letter (maximum 63 characters)",
            ));
        }
        Ok((at, name))
    }

    fn keyword(&mut self, expected: &str) -> Result<()> {
        let (at, keyword) = self.word()?;
        if keyword != expected {
            return Err(at.syntax(format!("expected '{expected}'")));
        }
        Ok(())
    }

    fn text(&mut self) -> Result<Name> {
        let (at, token) = self.next();
        match token {
            Token::Text(value) => Ok((at, value)),
            _ => Err(at.syntax("expected a quoted string")),
        }
    }

    fn path(&mut self) -> Result<PathBuf> {
        self.keyword("at")?;
        let (at, path) = self.text()?;
        if path.is_empty() || path.chars().any(char::is_control) {
            return Err(at.semantic("invalid local path"));
        }
        Ok(PathBuf::from(path))
    }

    fn filter(&mut self) -> Result<Filter> {
        let (at, kind) = self.word()?;
        let directory = match kind.as_str() {
            "file" => false,
            "directory" => true,
            _ => return Err(at.syntax("expected 'file' or 'directory'")),
        };
        let (at, source) = self.text()?;
        let parts = source.split('/').collect::<Vec<_>>();
        let mut pattern = "^".to_owned();
        for (index, part) in parts.iter().enumerate() {
            let last = index + 1 == parts.len();
            match *part {
                "" | "." | ".." => return Err(at.semantic("invalid relative glob")),
                "**" if last => pattern.push_str(".*"),
                "**" => pattern.push_str("(?:[^/]+/)*"),
                part if part.contains("**") || part.contains('\\') => {
                    return Err(at.semantic("use '/' and put '**' in its own path component"));
                }
                part => {
                    for c in part.chars() {
                        match c {
                            '*' => pattern.push_str("[^/]*"),
                            '?' => pattern.push_str("[^/]"),
                            _ => pattern.push_str(&regex::escape(&c.to_string())),
                        }
                    }
                    if !last {
                        pattern.push('/');
                    }
                }
            }
        }
        pattern.push('$');
        let pattern = Regex::new(&pattern).map_err(|error| at.semantic(error.to_string()))?;
        Ok(Filter {
            directory,
            pattern,
            source,
        })
    }

    fn list<T>(&mut self, item: impl Fn(&mut Self) -> Result<T>) -> Result<Vec<T>> {
        let mut items = vec![item(self)?];
        while self.eat(',') {
            items.push(item(self)?);
        }
        Ok(items)
    }

    fn grant(&mut self) -> Result<(Name, BTreeSet<Right>)> {
        let rights = self
            .list(Self::word)?
            .into_iter()
            .map(|(at, word)| {
                Right::parse(&word).ok_or_else(|| at.semantic(format!("unknown right '{word}'")))
            })
            .collect::<Result<BTreeSet<_>>>()?;
        self.keyword("on")?;
        let at = self.at();
        let share = if self.eat('*') {
            (at, "*".to_owned())
        } else {
            self.name()?
        };
        Ok((share, rights))
    }

    fn rule(&mut self, mode: Mode) -> Result<(Name, Rule)> {
        if matches!(mode, Mode::Archive | Mode::Receive) {
            self.keyword("from")?;
        }
        let device = self.name()?;
        let path = self.path()?;
        let mut propagate_delete = false;
        if mode.uploads() {
            self.keyword("on")?;
            self.keyword("delete")?;
            let (at, action) = self.word()?;
            match action.as_str() {
                "keep" => {}
                "propagate" if mode == Mode::Mirror => propagate_delete = true,
                _ => {
                    return Err(
                        at.semantic(format!("invalid deletion policy '{action}' for {mode:?}"))
                    );
                }
            }
            self.keyword("on")?;
            if mode == Mode::Mirror {
                self.keyword("conflict")?;
                self.keyword("keep-both")?;
            } else {
                self.keyword("overwrite")?;
                self.keyword("version")?;
            }
        }
        let mut filters = Vec::new();
        if self.eat('{') {
            while !self.eat('}') {
                self.keyword("ignore")?;
                filters.push(self.filter()?);
            }
        }
        Ok((
            device,
            Rule {
                mode,
                path,
                propagate_delete,
                filters,
            },
        ))
    }

    fn share(&mut self, at: Position) -> Result<ParsedShare> {
        self.symbol('{')?;
        let mut host = None;
        let mut filters = Vec::new();
        let mut rules = Vec::new();
        while !self.eat('}') {
            let (at, word) = self.word()?;
            match word.as_str() {
                "host" => {
                    let specification = (self.name()?, self.path()?);
                    if host.replace(specification).is_some() {
                        return Err(at.semantic("share has more than one host"));
                    }
                }
                "ignore" => filters.push(self.filter()?),
                "archive" => rules.push(self.rule(Mode::Archive)?),
                "receive" => rules.push(self.rule(Mode::Receive)?),
                "checkout" => rules.push(self.rule(Mode::Checkout)?),
                "mirror" => rules.push(self.rule(Mode::Mirror)?),
                _ => return Err(at.syntax(format!("unexpected share directive '{word}'"))),
            }
        }
        let (host, path) = host.ok_or_else(|| at.semantic("share has no host"))?;
        Ok(ParsedShare {
            host,
            path,
            filters,
            rules,
        })
    }

    fn invitation(&mut self) -> Result<ParsedInvitation> {
        self.symbol('{')?;
        self.keyword("issued")?;
        self.keyword("by")?;
        let issuers = self.list(Self::name)?;
        self.keyword("grant")?;
        let mut grants = vec![self.grant()?];
        while matches!(self.tokens.peek(), Some((_, Token::Word(word))) if word == "grant") {
            self.keyword("grant")?;
            grants.push(self.grant()?);
        }
        self.keyword("expires")?;
        self.keyword("in")?;
        let (at, duration) = self.word()?;
        let expires = duration
            .strip_suffix('m')
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|amount| (1..=10080).contains(amount))
            .ok_or_else(|| at.semantic("expiration must be between 1m and 10080m"))?
            * 60;
        self.keyword("uses")?;
        let (at, uses) = self.word()?;
        if uses != "1" {
            return Err(at.semantic("invitations must have exactly one use"));
        }
        self.symbol('}')?;
        Ok(ParsedInvitation {
            issuers,
            grants,
            expires,
        })
    }
}

struct ParsedShare {
    host: Name,
    path: PathBuf,
    filters: Vec<Filter>,
    rules: Vec<(Name, Rule)>,
}

struct ParsedInvitation {
    issuers: Vec<Name>,
    grants: Vec<(Name, BTreeSet<Right>)>,
    expires: u64,
}

pub fn parse(source: &str) -> Result<Network> {
    let mut parser = Parser {
        tokens: lex(source)?.into_iter().peekable(),
    };
    parser.keyword("network")?;
    let (network_at, name) = parser.name()?;
    parser.symbol('{')?;
    let mut devices = BTreeSet::new();
    let mut shares = Vec::new();
    let mut filters = Vec::new();
    let mut permits = Vec::new();
    let mut invitations = Vec::new();
    while !parser.eat('}') {
        let (at, directive) = parser.word()?;
        match directive.as_str() {
            "device" => {
                let (at, name) = parser.name()?;
                if !devices.insert(name.clone()) {
                    return Err(at.semantic(format!("duplicate device '{name}'")));
                }
            }
            "share" => {
                let name = parser.name()?;
                let share = parser.share(name.0)?;
                shares.push((name, share));
            }
            "ignore" => filters.push(parser.filter()?),
            "permit" => permits.push((parser.name()?, parser.grant()?)),
            "invitation" => invitations.push((parser.name()?, parser.invitation()?)),
            _ => return Err(at.syntax(format!("unexpected network directive '{directive}'"))),
        }
    }
    let (at, rest) = parser.next();
    if !matches!(rest, Token::End) {
        return Err(at.syntax("unexpected input after network"));
    }

    let known_device = |(at, name): &Name| {
        if !devices.contains(name) {
            Err(at.semantic(format!("unknown device '{name}'")))
        } else {
            Ok(())
        }
    };
    let mut host = None;
    let mut registered_shares = BTreeMap::new();
    for ((at, name), share) in shares {
        known_device(&share.host)?;
        let (host_at, share_host) = share.host;
        if host.as_ref().is_some_and(|host| host != &share_host) {
            return Err(host_at.semantic("all shares must use the same host"));
        }
        host = Some(share_host.clone());
        let mut rules = BTreeMap::new();
        for (device, rule) in share.rules {
            known_device(&device)?;
            let (at, device) = device;
            if device == share_host || rules.insert(device.clone(), rule).is_some() {
                return Err(at.semantic(format!(
                    "conflicting roles for '{device}' in share '{name}'"
                )));
            }
        }
        if registered_shares
            .insert(
                name.clone(),
                Share {
                    host: share_host,
                    path: share.path,
                    filters: share.filters,
                    rules,
                },
            )
            .is_some()
        {
            return Err(at.semantic(format!("duplicate share '{name}'")));
        }
    }
    let host = host.ok_or_else(|| network_at.semantic("network needs at least one share"))?;
    let mut registered_permits = BTreeMap::new();
    for (device, grant) in permits {
        known_device(&device)?;
        add_grant(
            &registered_shares,
            registered_permits.entry(device.1).or_default(),
            grant,
        )?;
    }
    let mut registered_invitations = BTreeMap::new();
    for ((at, name), invitation) in invitations {
        let mut issuers = BTreeSet::new();
        for issuer in invitation.issuers {
            known_device(&issuer)?;
            issuers.insert(issuer.1);
        }
        let mut grants = BTreeMap::new();
        for grant in invitation.grants {
            add_grant(&registered_shares, &mut grants, grant)?;
        }
        let invitation = Invitation {
            issuers,
            grants,
            expires: invitation.expires,
        };
        if registered_invitations
            .insert(name.clone(), invitation)
            .is_some()
        {
            return Err(at.semantic(format!("duplicate invitation '{name}'")));
        }
    }
    Ok(Network {
        name,
        host,
        devices,
        shares: registered_shares,
        filters,
        permits: registered_permits,
        invitations: registered_invitations,
    })
}

fn add_grant(
    shares: &BTreeMap<String, Share>,
    grants: &mut Grants,
    ((at, share), rights): (Name, BTreeSet<Right>),
) -> Result<()> {
    if share == "*" {
        for share in shares.keys() {
            grants.entry(share.clone()).or_default().extend(&rights);
        }
    } else {
        if !shares.contains_key(&share) {
            return Err(at.semantic(format!("unknown share '{share}'")));
        }
        grants.entry(share).or_default().extend(rights);
    }
    Ok(())
}

#[derive(Default, Deserialize, Serialize)]
pub struct Applied {
    pub source: String,
    pub base: PathBuf,
    pub device: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<PathBuf>,
}

impl Applied {
    pub fn read(file: &Path, device: String) -> Result<Self> {
        let file = file
            .canonicalize()
            .with_context(|| format!("opening {}", file.display()))?;
        let source = fs::read_to_string(&file)?;
        let base = file
            .parent()
            .context("config needs a parent directory")?
            .to_owned();
        let candidate = Self {
            source,
            base,
            device,
            file: Some(file),
        };
        candidate.network()?;
        Ok(candidate)
    }

    pub fn load(state: &Path) -> Result<Self> {
        let applied: Self = read_json(&state.join("applied.json"))?;
        ensure!(!applied.device.is_empty(), "apply a configuration first");
        Ok(applied)
    }

    pub fn network(&self) -> Result<Network> {
        let network = parse(&self.source)?;
        ensure!(
            network.devices.contains(&self.device),
            "unknown local device '{}'",
            self.device
        );
        Ok(network)
    }

    pub fn save(&self, state: &Path) -> Result<()> {
        let next = self.network()?;
        if state.join("applied.json").exists() {
            let previous = Self::load(state)?;
            ensure!(
                previous.device == self.device && previous.network()?.name == next.name,
                "this state belongs to another device or network"
            );
        }
        write_json(&state.join("applied.json"), self)
    }

    pub fn plan(&self) -> Result<String> {
        let network = self.network()?;
        let mut lines = vec![format!("network {} on {}", network.name, self.device)];
        for (name, share) in &network.shares {
            let rule = share.rules.get(&self.device);
            let (mode, path) = if self.device == share.host {
                ("Host".to_owned(), &share.path)
            } else if let Some(rule) = rule {
                (
                    format!(
                        "{:?}, delete {}",
                        rule.mode,
                        if rule.propagate_delete {
                            "propagate"
                        } else {
                            "keep"
                        }
                    ),
                    &rule.path,
                )
            } else {
                continue;
            };
            lines.push(format!(
                "  {name}: {mode} at {}",
                self.base.join(path).display()
            ));
            for filter in network
                .filters
                .iter()
                .chain(&share.filters)
                .chain(rule.into_iter().flat_map(|rule| &rule.filters))
            {
                lines.push(format!(
                    "    ignore {} {:?}",
                    if filter.directory {
                        "directory"
                    } else {
                        "file"
                    },
                    filter.source
                ));
            }
        }
        if self.device == network.host {
            for (device, grants) in &network.permits {
                lines.push(format!("  configured permissions for {device}: {grants:?}"));
            }
            for (name, invite) in &network.invitations {
                lines.push(format!(
                    "  invitation {name}: {:?}, {:?}, {}m, one use",
                    invite.issuers,
                    invite.grants,
                    invite.expires / 60
                ));
            }
        }
        lines.push("  apply saves the policy. File exchange starts with serve or sync.".into());
        Ok(lines.join("\n"))
    }
}
