// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The code generated for a command

use std::collections::HashSet;

use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::{Data, DeriveInput, Error, Fields, Ident, parse_quote, spanned::Spanned};

use crate::{
    args::{CmdArgs, FixedOption},
    field::field_option,
    types::option_list,
};

/// Collects several errors, reported together
#[derive(Default)]
struct Errors(Option<Error>);

impl Errors {
    fn push(&mut self, error: Error) {
        match &mut self.0 {
            Some(errors) => errors.combine(error),
            None => self.0 = Some(error),
        }
    }

    fn into_compile_error(self) -> TokenStream2 {
        self.0.map(Error::into_compile_error).unwrap_or_default()
    }
}

/// Expands the attribute. An error of the struct itself fails the expansion; the errors of the options are reported
/// with the expansion, which leaves the faulty options out, so that the struct is still usable.
pub(crate) fn expand(args: &CmdArgs, mut input: DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(Error::new(
            input.generics.span(),
            "admin commands cannot be generic",
        ));
    }
    let Data::Struct(data) = &mut input.data else {
        return Err(Error::new(
            input.ident.span(),
            "only structs can be admin commands",
        ));
    };
    let fields = match &mut data.fields {
        Fields::Named(fields) => fields.named.iter_mut().collect(),
        Fields::Unit => Vec::new(),
        Fields::Unnamed(fields) => {
            return Err(Error::new(
                fields.span(),
                "the fields of an admin command must be named",
            ));
        }
    };

    let mut errors = Errors::default();
    // The same key may be sent in two option lists (e.g. `Vid`, as a string and as a list of strings), but not twice in
    // the same one
    let mut keys = HashSet::new();
    let mut check_duplicate = |key: &Ident, list: String, errors: &mut Errors| {
        if !keys.insert((key.to_string(), list)) {
            errors.push(Error::new(
                key.span(),
                format!("the option `{key}` is sent twice"),
            ));
        }
    };

    let krate = quote!(::cta_client);
    let commands = quote!(#krate::admin::commands);
    let mut pushes = Vec::new();

    for field in fields {
        let option = match field_option(field) {
            Ok(option) => option,
            Err(error) => {
                errors.push(error);
                continue;
            }
        };
        check_duplicate(
            &option.key,
            option_list(&field.ty, option.only_if_true),
            &mut errors,
        );
        let name = field.ident.as_ref().expect("named field");
        let key = &option.key;
        let ty = &field.ty;
        pushes.push(if option.only_if_true {
            quote_spanned! {key.span()=>
                #commands::OptionValue::push(
                    #commands::Flag(value.#name), &mut cmd, <#commands::Flag as #commands::OptionValue>::Key::#key);
            }
        } else {
            quote_spanned! {key.span()=>
                #commands::OptionValue::push(value.#name, &mut cmd, <#ty as #commands::OptionValue>::Key::#key);
            }
        });
    }
    // The fixed options after the fields
    for fixed in &args.fixed {
        check_duplicate(&fixed.key, option_list(&fixed.ty, false), &mut errors);
        let FixedOption { key, ty, value } = fixed;
        pushes.push(quote_spanned! {key.span()=>
            {
                let value: #ty = #value;
                #commands::OptionValue::push(value, &mut cmd, <#ty as #commands::OptionValue>::Key::#key);
            }
        });
    }

    let errors = errors.into_compile_error();

    let builder_crate = quote!(#krate::__private::bon);
    input.attrs.push(parse_quote! {
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            ::core::cmp::PartialEq,
            ::core::cmp::Eq,
            #builder_crate::Builder
        )]
    });
    input
        .attrs
        .push(parse_quote!(#[builder(crate = #builder_crate, on(String, into))]));

    let protobuf = quote!(#krate::__private::cta_protobuf::cta);
    let name = &input.ident;
    let (cmd, sub_cmd) = (&args.cmd, &args.sub_cmd);
    let kind = match &args.response {
        None => quote!(impl #commands::UnaryAdminCmd for #name {}),
        Some(item) => quote! {
            impl #commands::StreamingAdminCmd for #name {
                type Item = #protobuf::admin::#item;
            }
        },
    };

    Ok(quote! {
        #input

        impl ::core::convert::From<#name> for #protobuf::admin::AdminCmd {
            #[allow(unused_variables, reason = "commands without options do not read the value")]
            fn from(value: #name) -> Self {
                #[allow(unused_mut, reason = "commands without options are not modified")]
                let mut cmd = #commands::new_admin_cmd(
                    <#name as #commands::TypedAdminCmd>::CMD,
                    <#name as #commands::TypedAdminCmd>::SUBCMD,
                );
                #(#pushes)*
                cmd
            }
        }

        impl #commands::TypedAdminCmd for #name {
            const CMD: #protobuf::admin::admin_cmd::Cmd = #protobuf::admin::admin_cmd::Cmd::#cmd;
            const SUBCMD: #protobuf::admin::admin_cmd::SubCmd = #protobuf::admin::admin_cmd::SubCmd::#sub_cmd;
        }

        #kind

        #errors
    })
}
