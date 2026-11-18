// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! What the tokens of a type tell about it (a proc macro sees no resolved types)

use quote::quote;
use syn::{GenericArgument, PathArguments, Type};

/// The option list of a type, as far as its tokens tell: the type without its `Option`s, flags being booleans.
pub(crate) fn option_list(ty: &Type, only_if_true: bool) -> String {
    let mut ty = ty;
    while let Some(inner) = option_inner(ty) {
        ty = inner;
    }
    let list = quote!(#ty).to_string();
    if only_if_true || list == "Flag" {
        "bool".to_owned()
    } else if list == "& str" {
        "String".to_owned()
    } else {
        list
    }
}

/// `T` of `Option<T>`
fn option_inner(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else { return None };
    let segment = path.path.segments.last()?;
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    match args.args.first() {
        Some(GenericArgument::Type(inner)) if segment.ident == "Option" && args.args.len() == 1 => {
            Some(inner)
        }
        _ => None,
    }
}

pub(crate) fn is_bool(ty: &Type) -> bool {
    matches!(ty, Type::Path(path) if path.qself.is_none() && path.path.is_ident("bool"))
}
