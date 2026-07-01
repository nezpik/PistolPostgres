-- Synthetic edtech schema for the PistolPostgres demo.
-- Deliberately ships with ONLY primary keys — no secondary indexes — so the
-- evolution loop has real, obvious optimization opportunities to discover.

DROP TABLE IF EXISTS public.activity_events, public.submissions, public.student_progress,
    public.assignments, public.enrollments, public.classes, public.students, public.schools CASCADE;

CREATE TABLE public.schools (
    id     BIGINT PRIMARY KEY,
    name   TEXT NOT NULL,
    region TEXT NOT NULL
);

CREATE TABLE public.students (
    id          BIGINT PRIMARY KEY,
    school_id   BIGINT NOT NULL,
    full_name   TEXT NOT NULL,
    grade_level INT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.classes (
    id         BIGINT PRIMARY KEY,
    school_id  BIGINT NOT NULL,
    subject    TEXT NOT NULL,
    teacher_id BIGINT NOT NULL
);

CREATE TABLE public.enrollments (
    id          BIGINT PRIMARY KEY,
    student_id  BIGINT NOT NULL,
    class_id    BIGINT NOT NULL,
    enrolled_at TIMESTAMPTZ NOT NULL,
    status      TEXT NOT NULL
);

CREATE TABLE public.assignments (
    id       BIGINT PRIMARY KEY,
    class_id BIGINT NOT NULL,
    title    TEXT NOT NULL,
    due_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.submissions (
    id            BIGINT PRIMARY KEY,
    assignment_id BIGINT NOT NULL,
    student_id    BIGINT NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL,
    graded        BOOLEAN NOT NULL,
    score         INT
);

CREATE TABLE public.student_progress (
    id         BIGINT PRIMARY KEY,
    student_id BIGINT NOT NULL,
    class_id   BIGINT NOT NULL,
    school_id  BIGINT NOT NULL,
    status     TEXT NOT NULL,
    score      INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE public.activity_events (
    id          BIGINT PRIMARY KEY,
    student_id  BIGINT NOT NULL,
    event_type  TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);
