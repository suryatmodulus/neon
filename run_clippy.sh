#!/bin/bash

# If you save this in your path under the name "cargo-zclippy" (or whatever
# name you like), then you can run it as "cargo zclippy" from the shell prompt.
#
# If your text editor has rust-analyzer integration, you can also use this new
# command as a replacement for "cargo check" or "cargo clippy" and see clippy
# warnings and errors right in the editor.
# In vscode, this setting is Rust-analyzer>Check On Save:Command

# manual-range-contains wants
#   !(4..=MAX_STARTUP_PACKET_LENGTH).contains(&len)
# instead of
#   len < 4 || len > MAX_STARTUP_PACKET_LENGTH
# , let's disagree.

# * `-A unknown_lints` – do not warn about unknown lint suppressions
#                        that people with newer toolchains might use
# * `-D warnings`      - fail on any warnings (`cargo` returns non-zero exit status)
cargo clippy --locked --all --all-targets --all-features -- -A unknown_lints -A clippy::manual-range-contains -D warnings
