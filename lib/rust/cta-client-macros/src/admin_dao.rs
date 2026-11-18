// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The code generated for a data access object (`#[admin_dao]`)

use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{Error, Fields, ItemStruct, parse_quote, spanned::Spanned};

/// Expands `#[admin_dao]`: gives the struct its `client` field and `Clone`, and `AdminClient` the accessor of the
/// struct.
pub(crate) fn expand(args: TokenStream2, mut input: ItemStruct) -> syn::Result<TokenStream2> {
    if !args.is_empty() {
        return Err(Error::new(args.span(), "`#[admin_dao]` takes no arguments"));
    }
    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            "data access objects cannot be generic",
        ));
    }
    if !matches!(input.fields, Fields::Unit) {
        return Err(Error::new(
            input.fields.span(),
            "a data access object must be a unit struct (`pub struct TapePools;`): \
             `#[admin_dao]` adds its `client` field",
        ));
    }

    let krate = quote!(::cta_client);
    let name = &input.ident;
    let vis = &input.vis;
    let accessor = format_ident!("{}", snake_case(&name.to_string()), span = name.span());
    // The accessor has the documentation of the struct
    let docs: Vec<_> = input
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .cloned()
        .collect();

    input.fields = Fields::Named(parse_quote!({ client: #krate::admin::AdminClient }));
    input.semi_token = None;
    input
        .attrs
        .push(parse_quote!(#[derive(::core::clone::Clone)]));

    Ok(quote! {
        #input

        impl #krate::admin::AdminClient {
            #(#docs)*
            #[must_use]
            #vis fn #accessor(&self) -> #name {
                #name {
                    client: ::core::clone::Clone::clone(self),
                }
            }
        }
    })
}

/// `TapePools` → `tape_pools`
fn snake_case(pascal: &str) -> String {
    let mut snake = String::with_capacity(pascal.len() + 4);
    for (i, c) in pascal.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                snake.push('_');
            }
            snake.extend(c.to_lowercase());
        } else {
            snake.push(c);
        }
    }
    snake
}

#[cfg(test)]
mod tests {
    use super::snake_case;

    #[test]
    fn snake_case_of_struct_names() {
        assert_eq!(snake_case("Tapes"), "tapes");
        assert_eq!(snake_case("TapePools"), "tape_pools");
        assert_eq!(snake_case("DiskInstanceSpaces"), "disk_instance_spaces");
        assert_eq!(snake_case("RecycleBinTapeFiles"), "recycle_bin_tape_files");
    }
}
