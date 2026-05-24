#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

print_header() {
  printf '\n==> %s\n' "$1"
}

fail() {
  printf '\nERROR: %s\n' "$1" >&2
  exit 1
}

print_header "Scanning production Rust code for runtime risk points"

rust_files=()
while IFS= read -r -d '' file; do
    rust_files+=("$file")
done < <(find src -type f -name '*.rs' -print0 | sort -z)
if [[ ${#rust_files[@]} -eq 0 ]]; then
  fail "no Rust source files found under src"
fi

risk_report="$(perl - "${rust_files[@]}" <<'PERL'
use strict;
use warnings;

my @patterns = (
    ["panic!", qr/\bpanic\s*!/],
    ["unwrap()", qr/\.unwrap\s*\(/],
    ["expect()", qr/\.expect(?:_err)?\s*\(/],
    ["todo!", qr/\btodo\s*!/],
    ["unimplemented!", qr/\bunimplemented\s*!/],
    ["unreachable!", qr/\bunreachable\s*!/],
);

sub brace_delta {
    my ($line) = @_;
    my $open = () = $line =~ /\{/g;
    my $close = () = $line =~ /\}/g;
    return $open - $close;
}

for my $file (@ARGV) {
    open my $fh, '<', $file or do {
        print "$file:0: read_error: $!\n";
        next;
    };

    my $pending_cfg_test = 0;
    my $skip_test_module = 0;
    my $brace_depth = 0;
    my $line_no = 0;

    while (my $line = <$fh>) {
        $line_no++;

        if ($skip_test_module) {
            $brace_depth += brace_delta($line);
            if ($brace_depth <= 0) {
                $skip_test_module = 0;
                $brace_depth = 0;
            }
            next;
        }

        if ($line =~ /^\s*#\[cfg\(test\)\]/) {
            $pending_cfg_test = 1;
            next;
        }

        if ($pending_cfg_test) {
            if ($line =~ /^\s*#\[/ || $line =~ /^\s*$/) {
                next;
            }
            if ($line =~ /^\s*mod\s+\w+\s*\{/) {
                $skip_test_module = 1;
                $brace_depth = brace_delta($line);
                if ($brace_depth <= 0) {
                    $skip_test_module = 0;
                    $brace_depth = 0;
                }
                $pending_cfg_test = 0;
                next;
            }
            $pending_cfg_test = 0;
        }

        next if $line =~ m{^\s*//[/!]};
        next if $line =~ m{^\s*//};

        my $code = $line;
        $code =~ s{//.*$}{};

        for my $entry (@patterns) {
            my ($label, $regex) = @$entry;
            if ($code =~ $regex) {
                chomp(my $snippet = $line);
                $snippet =~ s/^\s+//;
                print "$file:$line_no: $label: $snippet\n";
            }
        }
    }

    close $fh;
}
PERL
)"

if [[ -n "$risk_report" ]]; then
  printf '%s\n' "$risk_report" >&2
  fail "runtime risk scan failed; remove the reported panic/unwrap/expect-style risk points before running tests"
fi

printf 'Runtime risk scan passed.\n'

print_header "Checking compiler warnings as errors"
if ! RUSTFLAGS="-Dwarnings" cargo check --all-targets --all-features; then
  fail "warning check failed; cargo check emitted warnings or errors, so tests were not run"
fi

printf 'Warning check passed.\n'

print_header "Running all test targets, including test/ scenarios"
cargo test --all-targets -- --test-threads=1

print_header "All checks and tests completed successfully"
