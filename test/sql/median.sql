CREATE TABLE floatvals(val float, color text);

INSERT INTO floatvals VALUES
       (2.7790234, 'c'),
       (1.4, 'a');

SELECT * FROM floatvals ORDER BY val;
SELECT median(val) FROM floatvals;

INSERT INTO floatvals VALUES
       (-3.14159, 'b');

SELECT * FROM floatvals ORDER BY val;
SELECT median(val) FROM floatvals;

CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with even number of values
INSERT INTO intvals VALUES
       (0, 'a'),
       (1, 'b'),
       (-5, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd'),
       (8, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;
