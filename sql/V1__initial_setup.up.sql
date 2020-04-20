BEGIN;
CREATE TABLE orgs (
    id SERIAL PRIMARY KEY,
    name VARCHAR (200) NOT NULL,
    industry VARCHAR (200) NOT NULL,
    status VARCHAR (20) NOT NULL,
    created_at TIMESTAMP with time zone DEFAULT now() NOT NULL,
    updated_at TIMESTAMP with time zone NOT NULL
);
CREATE TABLE roles (
id SERIAL PRIMARY KEY,
org_id INTEGER NOT NULL,
description TEXT NOT NULL,
name text NOT NULL,
system_role boolean NOT NULL,
permissions TEXT NOT NULL,
created_at TIMESTAMP with time zone DEFAULT now() NOT NULL,
updated_at TIMESTAMP with time zone NOT NULL
);
COMMIT;

