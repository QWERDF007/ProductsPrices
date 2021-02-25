CREATE TABLE "products" (
	"pid"	TEXT NOT NULL,
	"href"	TEXT,
	"shop"	TEXT,
	"name"	TEXT,
	"min"	REAL,
	"over"  BLOB,
	"mtime" REAL,
	PRIMARY KEY("pid")
);

CREATE TABLE "prices" (
    "id"    INTEGER,
    "pid"   TEXT NOT NULL,
    "price" REAL,
    "ctime" TEXT,
    PRIMARY KEY("id" AUTOINCREMENT)
);
