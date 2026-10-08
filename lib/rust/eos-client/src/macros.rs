// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

/// The EOS API requires us to send the token as a GRPC protobuf field, rather
/// than the more standard request header. This helper macro makes it less verbose.
#[macro_export]
macro_rules! with_auth_key_from {
    ($auth:expr, $req:ident { $($field:ident: $value:expr),*$(,)? }) => {
        $req {
            authkey: $auth.token.clone(),
            $($field: $value,)*
        }
    };
}
