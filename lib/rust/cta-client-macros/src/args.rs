// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The arguments of the attribute: `#[typed_admin_cmd(Command::SubCommand, response = Record, with(Key = value))]`

use proc_macro2::Span;
use quote::format_ident;
use syn::{
    Error, Expr, Ident, Lit, Path, Token, Type, parenthesized,
    parse::{Parse, ParseStream, Parser},
    parse_quote,
    punctuated::Punctuated,
    spanned::Spanned,
};

/// The arguments of `#[typed_admin_cmd(Command::SubCommand, response = Record, with(Key = value))]`
pub(crate) struct CmdArgs {
    /// The command
    pub(crate) cmd: Ident,
    /// The subcommand, or `SubcmdNone` if the command has no subcommand
    pub(crate) sub_cmd: Ident,
    /// The options sent with a fixed value with every command
    pub(crate) fixed: Vec<FixedOption>,
    /// The record type of a streaming command
    pub(crate) response: Option<Ident>,
}

impl Parse for CmdArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Err(Error::new(
                Span::call_site(),
                "missing the command, e.g. `#[typed_admin_cmd(Tape::Ls)]`",
            ));
        }
        let (cmd, sub_cmd) = command(&input.parse()?)?;
        let mut args = Self {
            cmd,
            sub_cmd,
            fixed: Vec::new(),
            response: None,
        };
        if input.is_empty() {
            // no more arguments, return early
            return Ok(args);
        }
        input.parse::<Token![,]>()?;

        let mut with = false;
        let arg_parser = syn::meta::parser(|meta| {
            // `response = Record` or `with(Key = value, ...)`
            if meta.path.is_ident("response") {
                if args.response.is_some() {
                    return Err(meta.error("`response` given twice"));
                }
                args.response = Some(meta.value()?.parse()?);
                Ok(())
            } else if meta.path.is_ident("with") {
                if with {
                    return Err(meta.error("`with` given twice"));
                }
                with = true;
                let content;
                parenthesized!(content in meta.input);

                // parse the `Key = value` pairs in `with(...)`
                args.fixed
                    .extend(Punctuated::<FixedOption, Token![,]>::parse_terminated(
                        &content,
                    )?);
                Ok(())
            } else {
                Err(meta.error("expected `response = Record` or `with(Key = value, ...)`"))
            }
        });
        arg_parser.parse2(input.parse()?)?;
        Ok(args)
    }
}

/// The variants of `Cmd` and `SubCmd` named by `Command::SubCommand` (`SubCmd::Subcmd<SubCommand>`), or by `Command`
/// alone (`SubCmd::SubcmdNone`)
fn command(path: &Path) -> syn::Result<(Ident, Ident)> {
    let invalid = || {
        Error::new(
            path.span(),
            "expected `Command::SubCommand` (e.g. `Tape::Ls`) or `Command` (e.g. `Version`)",
        )
    };
    // the path must not start with `::` and must not have any generic arguments
    if path.leading_colon.is_some()
        || path
            .segments
            .iter()
            .any(|segment| !segment.arguments.is_none())
    {
        return Err(invalid());
    }
    let mut segments = path.segments.iter().map(|segment| &segment.ident);
    match (segments.next(), segments.next(), segments.next()) {
        // `Command` alone, no subcommand
        (Some(cmd), None, None) => Ok((cmd.clone(), Ident::new("SubcmdNone", cmd.span()))),
        // `Command::SubCommand`
        (Some(cmd), Some(sub_cmd), None) => Ok((
            cmd.clone(),
            format_ident!("Subcmd{}", sub_cmd, span = sub_cmd.span()),
        )),
        _ => Err(invalid()),
    }
}

/// `Key = value` or `Key: Type = value` in `with(...)`: an option sent with every command
pub(crate) struct FixedOption {
    pub(crate) key: Ident,
    pub(crate) ty: Type,
    pub(crate) value: Expr,
}

impl Parse for FixedOption {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key: Ident = input.parse()?;

        // optional type annotation: `Key: Type = value`
        let ty = if input.peek(Token![:]) {
            input.parse::<Token![:]>()?;
            Some(input.parse()?)
        } else {
            None
        };
        input.parse::<Token![=]>()?;
        let value: Expr = input.parse()?;
        let ty = match ty {
            Some(ty) => ty,
            // infer the type from the value, if possible
            None => literal_type(&value).ok_or_else(|| {
                Error::new(
                    value.span(),
                    format!(
                        "the type of this value cannot be inferred: write `{key}: Type = value`"
                    ),
                )
            })?,
        };
        Ok(Self { key, ty, value })
    }
}

/// The option type of a literal: `true` is a `bool`, `42` a `u64`, `"text"` a `&str`
fn literal_type(value: &Expr) -> Option<Type> {
    let Expr::Lit(lit) = value else { return None };
    match &lit.lit {
        Lit::Bool(_) => Some(parse_quote!(bool)),
        Lit::Int(_) => Some(parse_quote!(u64)),
        Lit::Str(_) => Some(parse_quote!(&str)),
        _ => None,
    }
}
