use anyhow::Result;
use nice_sync::language::{self, Mode};

#[test]
fn checks_the_three_sample_roles_and_filter_scopes() -> Result<()> {
    let network = language::parse(include_str!("../examples/network.sync"))?;
    assert_eq!(network.shares["image"].rules["phone"].mode, Mode::Archive);
    let mirror = &network.shares["docs"].rules["laptop"];
    assert_eq!(mirror.mode, Mode::Mirror);
    assert!(mirror.propagate_delete);
    let projects = &network.shares["projects"];
    let sender = &projects.rules["pc"];
    let checkout = &projects.rules["laptop"];
    assert_eq!(sender.mode, Mode::Receive);
    assert_eq!(checkout.mode, Mode::Checkout);
    for path in [".venv/py.exe", "nested/.venv/bin/py", ".venv"] {
        for rule in [sender, checkout] {
            assert!(network.ignored(projects, Some(rule), path, path == ".venv"));
        }
    }
    assert!(!network.ignored(projects, Some(sender), "model.gguf", false));
    assert!(network.ignored(projects, Some(checkout), "model.gguf", false));
    assert!(network.ignored(projects, Some(checkout), "models/model.gguf", false));
    assert!(!network.ignored(projects, Some(checkout), "model.gguf.txt", false));
    Ok(())
}

#[test]
fn distinguishes_diagnostic_categories_with_source_positions() {
    let syntax = language::parse(
        "network home {\n  device server\n  share photos { host server at \"unfinished\n}",
    )
    .unwrap_err()
    .to_string();
    assert!(syntax.starts_with("3:33: syntax:"), "{syntax}");
    let unknown = language::parse(
        "network home {\n  device server\n  share photos { host other at \"photos\" }\n}",
    )
    .unwrap_err()
    .to_string();
    assert!(
        unknown.starts_with("3:23: semantic: unknown device"),
        "{unknown}"
    );
    let sample = include_str!("../examples/network.sync");
    assert!(
        language::parse(&sample.replace(
            "on delete keep on overwrite",
            "on delete propagate on overwrite"
        ))
        .unwrap_err()
        .to_string()
        .contains("semantic: invalid deletion policy")
    );
    assert!(
        language::parse(&sample.replace("uses 1", "uses 2"))
            .unwrap_err()
            .to_string()
            .contains("semantic: invitations must have exactly one use")
    );
}
