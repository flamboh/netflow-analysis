#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../.." && pwd)"
burstify_dir="$repo_root/vendor/burstify"
structure_output="$burstify_dir/zig-out/bin/StructureFunction"

if [ ! -d "$burstify_dir" ] || [ ! -f "$burstify_dir/build.zig" ]; then
	echo "vendor/burstify not initialized; run: git submodule update --init --recursive" >&2
	exit 1
fi

cd "$burstify_dir"

if ! command -v zig >/dev/null 2>&1; then
	echo "zig not found; install the dependencies from vendor/burstify/shell.nix or use nix-shell." >&2
	exit 1
fi

mkdir -p "$(dirname "$structure_output")"

temp_source="$(mktemp "$burstify_dir/.structure-function.XXXXXX.zig")"
trap 'rm -f "$temp_source"' EXIT

cat >"$temp_source" <<'EOF'
const std = @import("std");

const addr = @import("src/addrs.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    defer {
        const deinit_status = gpa.deinit();
        if (deinit_status == .leak) @panic("TEST FAIL: leaked memory");
    }

    const stdin = std.fs.File.stdin();
    const stdout = std.fs.File.stdout();
    var buffer: [1024]u8 = undefined;

    var pfxs = try addr.AddrAnalyzer.init(allocator);
    defer pfxs.deinit();

    const reader = stdin.deprecatedReader();
    while (true) {
        if (try nextLine(reader, &buffer)) |line| {
            const ipv4 = try addr.string_to_ipv4(line);
            try pfxs.addAddr(ipv4);
        } else {
            break;
        }
    }

    const structure_function = try pfxs.structure_function(allocator);
    defer allocator.free(structure_function);

    const writer = stdout.deprecatedWriter();
    try writer.print("q,tau,sd\n", .{});
    for (structure_function) |elem| {
        try writer.print("{d},{d},{d}\n", .{ elem.@"0", elem.@"1", elem.@"2" });
    }
}

fn nextLine(reader: anytype, buffer: []u8) !?[]const u8 {
    const line = (try reader.readUntilDelimiterOrEof(buffer, '\n')) orelse return null;
    if (@import("builtin").os.tag == .windows) {
        return std.mem.trimRight(u8, line, "\r");
    }
    return line;
}
EOF

zig build-exe "$temp_source" -O ReleaseFast -femit-bin="$structure_output"

if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libpcap; then
	exec zig build
fi

echo "Built $structure_output"
echo "Skipped full burstify build because libpcap is unavailable."
