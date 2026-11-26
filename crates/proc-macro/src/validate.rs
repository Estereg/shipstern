//! Expansion-time checks that turn a broken IDL into a `compile_error!` naming the
//! offending entries, instead of generated code that fails with duplicate-definition
//! errors that do not name them, or a parser arm that silently never matches.
//!
//! The discriminator-width checks fail loudly where the discriminator-constant guards
//! skip silently. A skipped constant only withholds an extra, and the IDL compiles as
//! before. A width mismatch leaves an account or instruction that can never parse,
//! with no diagnostic at runtime.

use std::collections::HashMap;

use codama_nodes::{DiscriminatorNode, EventNode, RootNode};

use crate::utils::{to_pascal_case, to_snake_case};

pub(crate) fn validate_idl(
    idl: &RootNode,
    events: &[EventNode],
    envelope: Option<&crate::parse::ProgramEnvelope>,
) -> Result<(), String> {
    check_names(idl, events)?;

    check_discriminator_widths(idl, envelope)
}

fn check_names(idl: &RootNode, events: &[EventNode]) -> Result<(), String> {
    let instruction_names = idl.program.instructions.iter().map(|ix| ix.name.as_str());

    check_unique("instruction", instruction_names, |name| {
        let pascal = to_pascal_case(name);

        vec![
            format!("{pascal}Accounts"),
            format!("{pascal}Args"),
            pascal,
            format!("parse_{}", to_snake_case(name)),
        ]
    })?;

    check_unique(
        "account",
        idl.program
            .accounts
            .iter()
            .map(|account| account.name.as_str()),
        |name| vec![to_pascal_case(name)],
    )?;

    // Events are only emitted under `program-events`; checking them otherwise would
    // reject IDLs that compile today.
    if cfg!(feature = "program-events") {
        check_unique(
            "event",
            events.iter().map(|event| event.name.as_str()),
            |name| {
                let pascal = to_pascal_case(name);

                vec![
                    format!("{pascal}Accounts"),
                    format!("{pascal}Args"),
                    pascal,
                    format!("parse_event_{}", to_snake_case(name)),
                ]
            },
        )?;
    }

    Ok(())
}

/// Names that differ only by ASCII case or leading underscores fold to one
/// identifier, and the identifier set is wider than the pascal-case variant: a
/// pascal collision does not imply a snake one (`_abc` and `abc`).
fn check_unique<'a>(
    kind: &str,
    names: impl Iterator<Item = &'a str>,
    idents: impl Fn(&str) -> Vec<String>,
) -> Result<(), String> {
    let mut seen: HashMap<String, &str> = HashMap::new();

    for name in names {
        for ident in idents(name) {
            let Some(first) = seen.insert(ident.clone(), name) else {
                continue;
            };

            return Err(format!(
                "{kind}s `{first}` and `{name}` both generate the identifier `{ident}`; rename \
                 one of them in the IDL",
            ));
        }
    }

    Ok(())
}

fn check_discriminator_widths(
    idl: &RootNode,
    envelope: Option<&crate::parse::ProgramEnvelope>,
) -> Result<(), String> {
    for ix in &idl.program.instructions {
        let Some((size, len)) = crate::render::instruction_parser::dead_width_discriminator(ix)
        else {
            continue;
        };

        return Err(width_message("instruction", &ix.name, size, len));
    }

    // The account parser reads only the first discriminator, so later ones are
    // never checked.
    for account in &idl.program.accounts {
        let Some(DiscriminatorNode::Field(node)) = account.discriminators.first() else {
            continue;
        };

        let Some(field) = crate::render::account_parser::field_discriminator(account, node) else {
            continue;
        };

        // An empty default has no bytes to compare, so the arm is a catch-all that
        // matches any data long enough to hold the field.
        if field.bytes.is_empty() || !crate::utils::width_mismatch(field.size, &field.bytes) {
            continue;
        }

        return Err(width_message(
            "account",
            &account.name,
            field.size,
            field.bytes.len(),
        ));
    }

    // Events are only parsed under `program-events`; checking them otherwise would
    // reject IDLs that compile today.
    if !cfg!(feature = "program-events") {
        return Ok(());
    }

    let events = &idl.program.events;

    let event_envelopes = envelope.map_or_else(
        || vec![None; events.len()],
        |program| program.resolve(events),
    );

    for (event, envelope) in events.iter().zip(event_envelopes) {
        let Some((size, len)) =
            crate::render::instruction_parser::dead_width_event_discriminator(event, envelope)
        else {
            continue;
        };

        return Err(width_message("event", &event.name, size, len));
    }

    Ok(())
}

fn width_message(kind: &str, name: &str, size: usize, len: usize) -> String {
    format!(
        "{kind} `{name}` declares a discriminator field of {size} bytes but its default value is \
         {len} bytes, so it can never match; make the declared size and the default agree",
    )
}
