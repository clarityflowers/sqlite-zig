const std = @import("std");
const panic = @import("builtin").panic;
const Allocator = std.mem.Allocator;
pub const errors = @import("error.zig");
pub const Error = errors.Error;
pub const Success = errors.Success;
pub const checkSqliteErr = errors.checkSqliteErr;
const bind = @import("bind.zig");

usingnamespace @import("c.zig");

/// Workaround Zig translate-c not being able to translate SQLITE_TRANSIENT into an actual value
const S: isize = -1;
const ZIG_SQLITE_TRANSIENT: fn (?*c_void) callconv(.C) void = @intToPtr(fn (?*c_void) callconv(.C) void, @bitCast(usize, S));

pub const Database = struct {
    db: *sqlite3,

    pub fn open(filename: [:0]const u8) Error!@This() {
        var db: ?*sqlite3 = undefined;

        var rc = sqlite3_open(filename, &db);
        errdefer errors.assertOkay(sqlite3_close(db));

        _ = try checkSqliteErr(rc);

        var dbNonNull = db orelse panic("No error, sqlite db should not be null", null);

        return @This(){
            .db = dbNonNull,
        };
    }

    pub const OpenOptions = struct {
        mode: enum { readonly, readwrite, readwrite_create } = .readwrite_create,
        interpret_as_uri: bool = false,
        in_memory: bool = false,
        threading: ?enum { no_mutex, full_mutex } = null,
        cache: ?enum { shared, private } = null,
        vfs_module: ?[:0]const u8 = null,
    };

    pub fn openWithOptions(filename: [:0]const u8, options: OpenOptions) Error!@This() {
        var db: ?*sqlite3 = undefined;

        var option_flags: c_int = switch (options.mode) {
            .readonly => SQLITE_OPEN_READONLY,
            .readwrite => SQLITE_OPEN_READWRITE,
            .readwrite_create => SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
        };
        if (options.interpret_as_uri) option_flags |= SQLITE_OPEN_URI;
        if (options.in_memory) option_flags |= SQLITE_OPEN_MEMORY;
        if (options.threading) |threading| {
            option_flags |= @as(c_int, switch (threading) {
                .no_mutex => SQLITE_OPEN_NOMUTEX,
                .full_mutex => SQLITE_OPEN_FULLMUTEX,
            });
        }
        if (options.cache) |cache| {
            option_flags |= @as(c_int, switch (cache) {
                .shared => SQLITE_OPEN_SHAREDCACHE,
                .private => SQLITE_OPEN_PRIVATECACHE,
            });
        }

        var rc = sqlite3_open_v2(filename, &db, option_flags, options.vfs_module orelse 0);
        errdefer errors.assertOkay(sqlite3_close(db));

        _ = try checkSqliteErr(rc);

        var dbNonNull = db orelse panic("No error, sqlite db should not be null", null);

        return @This(){
            .db = dbNonNull,
        };
    }

    pub fn close(self: *const @This()) Error!void {
        _ = try checkSqliteErr(sqlite3_close(self.db));
    }

    pub fn errmsg(self: *const @This()) ?[*:0]const u8 {
        return sqlite3_errmsg(self.db);
    }

    pub fn prepare(self: *const @This(), sql: [:0]const u8, sqlTail: ?*[:0]const u8) Error!?Statement {
        var stmt: ?*sqlite3_stmt = null;
        const sqlLen = @intCast(c_int, sql.len + 1);
        var tail: ?[*]u8 = undefined;

        var rc = sqlite3_prepare_v2(self.db, sql, sqlLen, &stmt, &tail);

        _ = try checkSqliteErr(rc);

        if (tail) |cTail| {
            if (sqlTail) |sqlTailNotNull| {
                const offset = @ptrToInt(cTail) - @ptrToInt(sql.ptr);
                sqlTailNotNull.* = sql[offset..];
            }
        }

        return Statement{
            .stmt = stmt orelse return null,
        };
    }

    pub fn exec(self: *const @This(), sql: [:0]const u8) RowsIterator {
        return RowsIterator.init(self, sql);
    }

    pub fn execBind(self: *const @This(), comptime sql: [:0]const u8, args: anytype) !RowsIterator {
        var tail: [:0]const u8 = sql;
        var stmtOpt = try self.prepare(sql, &tail);
        if (stmtOpt) |stmt| {
            errdefer stmt.finalize() catch {};
            try bind.bind(&stmt, sql, args);
        }
        return RowsIterator{ .db = self, .remaingSql = tail, .stmt = stmtOpt };
    }
};

pub const Statement = struct {
    stmt: *sqlite3_stmt,

    pub fn step(self: *const Statement) Error!Success {
        return try checkSqliteErr(sqlite3_step(self.stmt));
    }

    pub fn columnCount(self: *const Statement) c_int {
        return sqlite3_column_count(self.stmt);
    }

    pub fn columnType(self: *const Statement, col: c_int) FieldTypeTag {
        switch (sqlite3_column_type(self.stmt, col)) {
            SQLITE_INTEGER => return .Integer,
            SQLITE_FLOAT => return .Float,
            SQLITE_TEXT => return .Text,
            SQLITE_BLOB => return .Blob,
            SQLITE_NULL => return .Null,
            else => panic("Unexpected sqlite datatype", null),
        }
    }

    pub fn column(self: *const Statement, col: c_int) FieldType {
        switch (self.columnType(col)) {
            .Integer => return FieldType{ .Integer = self.columnInt64(col) },
            .Float => return FieldType{ .Float = self.columnFloat(col) },
            .Text => return FieldType{ .Text = self.columnText(col) },
            .Blob => return FieldType{ .Blob = self.columnBlob(col) },
            .Null => return FieldType{ .Null = {} },
        }
    }

    pub fn columnInt(self: *const Statement, col: c_int) i32 {
        return sqlite3_column_int(self.stmt, col);
    }

    pub fn columnInt64(self: *const Statement, col: c_int) i64 {
        return sqlite3_column_int64(self.stmt, col);
    }

    pub fn columnFloat(self: *const Statement, col: c_int) f64 {
        return sqlite3_column_double(self.stmt, col);
    }

    pub fn columnText(self: *const Statement, col: c_int) []const u8 {
        const num_bytes = sqlite3_column_bytes(self.stmt, col);
        const bytes = sqlite3_column_text(self.stmt, col);
        return bytes[0..@intCast(usize, num_bytes)];
    }

    pub fn columnBlob(self: *const Statement, col: c_int) []const u8 {
        const num_bytes = sqlite3_column_bytes(self.stmt, col);
        const bytes = @ptrCast([*]const u8, sqlite3_column_blob(self.stmt, col));
        return bytes[0..@intCast(usize, num_bytes)];
    }

    pub fn bindInt64(self: *const Statement, paramIdx: c_int, number: i64) Error!void {
        _ = try checkSqliteErr(sqlite3_bind_int64(self.stmt, paramIdx, number));
    }

    pub fn bindText(self: *const Statement, paramIdx: c_int, text: []const u8) Error!void {
        _ = try checkSqliteErr(sqlite3_bind_text(self.stmt, paramIdx, text.ptr, @intCast(c_int, text.len), ZIG_SQLITE_TRANSIENT));
    }

    pub fn finalize(self: *const Statement) Error!void {
        _ = try checkSqliteErr(sqlite3_finalize(self.stmt));
    }
};

pub const FieldTypeTag = enum {
    Integer,
    Float,
    Text,
    Blob,
    Null,
};

pub const FieldType = union(FieldTypeTag) {
    Integer: i64,
    Float: f64,
    Text: []const u8,
    Blob: []const u8,
    Null: void,

    pub fn int(number: i64) @This() {
        return .{ .Integer = number };
    }

    pub fn text(str: []const u8) @This() {
        return .{ .Text = str };
    }

    pub fn eql(self: *const FieldType, other: *const FieldType) bool {
        if (@as(FieldTypeTag, self.*) != @as(FieldTypeTag, other.*)) {
            return false;
        }
        switch (self.*) {
            // Types must be same, and any null is the same as any other null
            .Null => return true,
            .Integer => return self.Integer == other.Integer,
            .Float => return self.Float == other.Float,
            .Text => return std.mem.eql(u8, self.Text, other.Text),
            .Blob => return std.mem.eql(u8, self.Blob, other.Blob),
        }
    }
};

pub const Row = struct {
    stmt: Statement,

    pub fn columnCount(self: *const @This()) c_int {
        return self.stmt.columnCount();
    }

    pub fn column(self: *const @This(), col: c_int) FieldType {
        return self.stmt.column(col);
    }

    pub fn columnInt(self: *const @This(), col: c_int) i32 {
        return self.stmt.columnInt(col);
    }

    pub fn columnInt64(self: *const @This(), col: c_int) i64 {
        return self.stmt.columnInt64(col);
    }

    pub fn columnText(self: *const @This(), col: c_int) []const u8 {
        return self.stmt.columnText(col);
    }
};

pub const RowsIterator = struct {
    db: *const Database,
    remaingSql: [:0]const u8,
    stmt: ?Statement = null,

    pub fn init(db: *const Database, sql: [:0]const u8) RowsIterator {
        var self: @This() = .{
            .db = db,
            .remaingSql = sql,
        };
        return self;
    }

    pub const Item = union(enum) {
        Row: Row,
        Done: void,
        Error: Error,
    };

    pub fn next(self: *@This()) ?Item {
        if (self.stmt == null) {
            self.prepareNextStmt() catch |e| return Item{ .Error = e };
        }
        if (self.stmt) |stmt| {
            const step_res = stmt.step() catch |e| return Item{ .Error = e };
            switch (step_res) {
                .Row, .Ok => return Item{ .Row = Row{ .stmt = stmt } },
                .Done => {
                    self.finalizeStmt() catch |e| return Item{ .Error = e };
                    return Item{ .Done = .{} };
                },
            }
        } else {
            return null;
        }
    }

    pub fn finish(self: *@This()) !void {
        while (self.next()) |item| {
            switch (item) {
                .Error => |e| return e,
                else => {},
            }
        }
    }

    fn finalizeStmt(self: *@This()) !void {
        if (self.stmt) |stmt| {
            try stmt.finalize();
            self.stmt = null;
        }
    }

    fn prepareNextStmt(self: *@This()) !void {
        if (self.stmt) |stmt| {
            try self.finalizeStmt();
        }
        const curSql = self.remaingSql;
        self.stmt = try self.db.prepare(curSql, &self.remaingSql);
    }
};

test "open in memory sqlite db" {
    const db = try Database.open(":memory:");

    // Create the hello table
    const sqlCreateTable = "CREATE TABLE hello (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);";
    const create_stmt = (try db.prepare(sqlCreateTable, null)) orelse return error.NullCreateStmt;
    _ = try create_stmt.step();
    _ = try create_stmt.finalize();

    // Insert values and get results
    const sql =
        \\ INSERT INTO hello (name) VALUES ("world"), ("foo");
        \\ SELECT * FROM hello;
    ;
    var tailSql: [:0]const u8 = sql;

    while (true) {
        const curSql = tailSql;

        const cur_stmt = (try db.prepare(curSql, &tailSql)) orelse break;
        var row: usize = 0;
        while ((try cur_stmt.step()) != .Done) {
            var col: c_int = 0;
            while (col < cur_stmt.columnCount()) {
                const val = cur_stmt.column(col);
                switch (row) {
                    0 => switch (col) {
                        0 => std.testing.expectEqual(FieldType{ .Integer = 1 }, val),
                        1 => std.testing.expect(FieldType.text("world").eql(&val)),
                        else => panic("unexpected col in test", null),
                    },
                    1 => switch (col) {
                        0 => std.testing.expectEqual(FieldType{ .Integer = 2 }, val),
                        1 => std.testing.expect(FieldType.text("foo").eql(&val)),
                        else => panic("unexpected col in test", null),
                    },
                    else => panic("unexpected row in test", null),
                }
                col += 1;
            }
            row += 1;
        }
        try cur_stmt.finalize();
    }

    try db.close();
}

test "Empty SQL prepared" {
    const db = try Database.open(":memory:");
    const create_stmt = try db.prepare("", null);
    std.debug.assert(create_stmt == null);

    try db.close();
}

test "exec function" {
    const db = try Database.open(":memory:");

    // Create the hello table
    try db.exec("CREATE TABLE hello (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);").finish();
    // Insert values and get results
    try db.exec("INSERT INTO hello (name) VALUES (\"world\"), (\"foo\");").finish();

    const expected = [_][2]FieldType{
        .{ FieldType.int(1), FieldType.text("world") },
        .{ FieldType.int(2), FieldType.text("foo") },
    };

    var rows = db.exec("SELECT * FROM hello;");

    var rowIdx: usize = 0;
    while (rows.next()) |rows_item| {
        const row = switch (rows_item) {
            .Row => |r| r,
            .Done => continue,
            .Error => panic("Error unwrapping row", null),
        };
        const expectedRow = expected[rowIdx];

        var colIdx: usize = 0;
        while (colIdx < row.columnCount()) {
            const col = row.column(@intCast(c_int, colIdx));
            const expectedCol = expectedRow[colIdx];
            std.testing.expect(expectedCol.eql(&col));

            colIdx += 1;
        }

        rowIdx += 1;
    }

    try db.close();
}

test "exec multiple statement" {
    const db = try Database.open(":memory:");

    const expected = [_][2]FieldType{
        .{ FieldType.int(1), FieldType.text("world") },
        .{ FieldType.int(2), FieldType.text("foo") },
    };

    // Create the hello table, insert test values, and get results
    var rows = db.exec(
        \\ CREATE TABLE hello (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);
        \\ INSERT INTO hello (name) VALUES ("world"), ("foo");
        \\ SELECT * FROM hello;
    );

    var rowIdx: usize = 0;
    while (rows.next()) |rows_item| {
        const row = switch (rows_item) {
            .Row => |r| r,
            .Done => continue,
            .Error => |e| return e,
        };
        const expectedRow = expected[rowIdx];

        var colIdx: usize = 0;
        while (colIdx < row.columnCount()) {
            const col = row.column(@intCast(c_int, colIdx));
            const expectedCol = expectedRow[colIdx];
            std.testing.expect(expectedCol.eql(&col));

            colIdx += 1;
        }

        rowIdx += 1;
    }

    std.testing.expectEqual(@as(usize, 2), rowIdx);

    try db.close();
}

test "bind parameters" {
    const db = try Database.open(":memory:");
    try db.exec("CREATE TABLE hello (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);").finish();

    const NAME = "world!";
    const NAME2 = "foo";

    const insert = (try db.prepare("INSERT INTO hello (name) VALUES (?);", null)).?;
    try insert.bindText(1, NAME);
    _ = try insert.step();
    try insert.finalize();

    try (try db.execBind("INSERT INTO hello (name) VALUES (?);", .{NAME2})).finish();

    var rows = db.exec("SELECT name FROM hello;");
    std.testing.expect(rows.next().?.Row.column(0).eql(&FieldType.text(NAME)));
    std.testing.expect(rows.next().?.Row.column(0).eql(&FieldType.text(NAME2)));
    std.testing.expect(rows.next().? == .Done);
    std.testing.expect(rows.next() == null);

    try db.close();
}
