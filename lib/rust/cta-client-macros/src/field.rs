// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The fields of a command and their `#[proto(...)]` attribute: the option each one sends

use proc_macro2::Span;
use quote::format_ident;
use syn::{
    Attribute, Data, DeriveInput, Error, Field, Ident, Meta, Token,
    ext::IdentExt,
    parse::{Parse, ParseStream},
    parse_quote_spanned,
    punctuated::Punctuated,
    spanned::Spanned,
};

use crate::types::is_bool;

/// What a field sends, from its `#[proto(...)]` attribute (if any)
pub(crate) struct FieldOption {
    /// The option key: given, or the name of the field in `PascalCase`
    pub(crate) key: Ident,
    /// Sent as a `Flag`
    pub(crate) only_if_true: bool,
}

/// One argument of `#[proto(...)]`: an option key (`TapePool`) or `only_if_true`
enum ProtoArg {
    Key(Ident),
    OnlyIfTrue(Span),
}

impl Parse for ProtoArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        if ident == "only_if_true" {
            Ok(Self::OnlyIfTrue(ident.span()))
        } else if ident
            .to_string()
            .starts_with(|c: char| c.is_ascii_uppercase())
        {
            Ok(Self::Key(ident))
        } else {
            Err(Error::new(
                ident.span(),
                "expected an option key (e.g. `TapePool`) or `only_if_true`",
            ))
        }
    }
}

/// Reads (and removes) the `#[proto(...)]` attribute of a field; an `only_if_true` one also gets `#[builder(default)]`.
pub(crate) fn field_option(field: &mut Field) -> syn::Result<FieldOption> {
    let name = field.ident.as_ref().expect("named field");
    let (proto, others): (Vec<Attribute>, Vec<Attribute>) = field
        .attrs
        .drain(..)
        .partition(|attr| attr.path().is_ident("proto"));
    field.attrs = others;

    let mut key = None;
    let mut only_if_true = None;
    if let Some(extra) = proto.get(1) {
        return Err(Error::new(extra.span(), "`#[proto]` given twice"));
    }
    if let Some(attr) = proto.first() {
        for arg in attr.parse_args_with(Punctuated::<ProtoArg, Token![,]>::parse_terminated)? {
            match arg {
                ProtoArg::Key(ident) if key.is_some() => {
                    return Err(Error::new(ident.span(), "only one option key per field"));
                }
                ProtoArg::Key(ident) => key = Some(ident),
                ProtoArg::OnlyIfTrue(span) if only_if_true.is_some() => {
                    return Err(Error::new(span, "`only_if_true` given twice"));
                }
                ProtoArg::OnlyIfTrue(span) => only_if_true = Some(span),
            }
        }
    }

    if let Some(span) = only_if_true {
        if !is_bool(&field.ty) {
            return Err(Error::new(
                field.ty.span(),
                "an `only_if_true` field must be a `bool`",
            ));
        }
        if let Some(attr) = field.attrs.iter().find(|attr| is_builder_default(attr)) {
            return Err(Error::new(
                attr.span(),
                "redundant `#[builder(default)]`: `only_if_true` implies it",
            ));
        }
        field
            .attrs
            .push(parse_quote_spanned!(span=> #[builder(default)]));
    }

    Ok(FieldOption {
        key: key.unwrap_or_else(|| {
            format_ident!(
                "{}",
                pascal_case(&name.unraw().to_string()),
                span = name.span()
            )
        }),
        only_if_true: only_if_true.is_some(),
    })
}

/// Whether an attribute is a `#[builder(...)]` with a `default`
fn is_builder_default(attr: &Attribute) -> bool {
    match &attr.meta {
        Meta::List(list) if list.path.is_ident("builder") => list.tokens.clone().into_iter().any(
            |token| matches!(token, proc_macro2::TokenTree::Ident(ident) if ident == "default"),
        ),
        _ => false,
    }
}

/// Removes the `#[proto]` attributes of the fields, so that the struct still compiles after an error.
pub(crate) fn strip_proto_attrs(input: &mut DeriveInput) {
    if let Data::Struct(data) = &mut input.data {
        for field in &mut data.fields {
            field.attrs.retain(|attr| !attr.path().is_ident("proto"));
        }
    }
}

/// `tape_pool` → `TapePool`
fn pascal_case(snake: &str) -> String {
    snake
        .split('_')
        .map(|word| {
            let mut chars = word.chars();
            chars.next().map_or_else(String::new, |first| {
                first.to_uppercase().chain(chars).collect()
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::pascal_case;

    #[test]
    fn pascal_case_of_field_names() {
        assert_eq!(pascal_case("vid"), "Vid");
        assert_eq!(pascal_case("tape_pool"), "TapePool");
        assert_eq!(pascal_case("max_drives_allowed"), "MaxDrivesAllowed");
        assert_eq!(pascal_case("disk_instance_space"), "DiskInstanceSpace");
    }
}
