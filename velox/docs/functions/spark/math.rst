======================
Mathematical Functions
======================

.. spark:function:: abs(x) -> [same as x]

    Returns the absolute value of ``x``. When ``x`` is negative minimum
    value of integral type, returns the same value as ``x`` following
    the behavior when Spark ANSI mode is disabled.

.. spark:function:: acos(x) -> double

    Returns the inverse cosine (a.k.a. arc cosine) of ``x``.

.. spark:function:: acosh(x) -> double

    Returns inverse hyperbolic cosine of ``x``.

.. spark:function:: asin(x) -> double

    Returns the arc sine of ``x``.

.. spark:function:: asinh(x) -> double

    Returns inverse hyperbolic sine of ``x``.

.. spark:function:: atan(x) -> double

    Returns the arc tangent of ``x``.

.. spark:function:: atan2(y, x) -> double

    Returns the arc tangent of ``y / x``. For compatibility with Spark, returns 0 for the following corner cases:

    * atan2(0.0, 0.0)
    * atan2(-0.0, -0.0)
    * atan2(-0.0, 0.0)
    * atan2(0.0, -0.0)

.. spark:function:: atanh(x) -> double

    Returns inverse hyperbolic tangent of ``x``.

.. spark:function:: add(x, y) -> [same as x]

    Returns the result of adding x to y. The types of x and y must be the same.
    Corresponds to sparks's operator ``+``.

.. spark:function:: add(x, y) -> decimal

    Returns the result of adding ``x`` to ``y``. The argument types should be DECIMAL, and can have different precisions and scales.
    Fast path is implemented for cases that should not overflow. For the others, the whole parts and fractional parts of input decimals are added separately and combined finally.
    The result type is calculated with the max precision of input precisions, the max scale of input scales, and one extra digit for possible carrier.
    Overflow results in null output. Corresponds to Spark's operator ``+``.

    ::

        SELECT CAST(1.1232100 as DECIMAL(38, 7)) + CAST(1 as DECIMAL(10, 0)); -- DECIMAL(38, 6) 2.123210
        SELECT CAST(-999999999999999999999999999.999 as DECIMAL(30, 3)) + CAST(-999999999999999999999999999.999 as DECIMAL(30, 3)); -- DECIMAL(31, 3) -1999999999999999999999999999.998
        SELECT CAST(99999999999999999999999999999999.99998 as DECIMAL(38, 6)) + CAST(-99999999999999999999999999999999.99999 as DECIMAL(38, 5)); -- DECIMAL(38, 6) -0.000010
        SELECT CAST(-99999999999999999999999999999999990.0 as DECIMAL(38, 3)) + CAST(-0.00001 as DECIMAL(38, 7)); -- DECIMAL(38, 6) NULL

.. spark:function:: bin(x) -> varchar

    Returns the string representation of the long value ``x`` represented in binary.

.. spark:function:: cbrt(x) -> double

    Returns the cube root of ``x``.

.. spark:function:: ceil(x) -> [same as x]

    Returns ``x`` rounded up to the nearest integer.
    Supported types are: BIGINT, DOUBLE and DECIMAL.

.. function:: checked_add(x, y) -> [same as x]

    Returns the result of adding x to y. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to Spark's operator ``+`` with ``failOnError`` as true.

.. function:: checked_divide(x, y) -> [same as x]

    Returns the results of dividing x by y. The types of x and y must be the same.
    Division by zero results in an error. Corresponds to Spark's operator ``/`` with ``failOnError`` as true.

.. function:: checked_multiply(x, y) -> [same as x]

    Returns the result of multiplying x by y. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to Spark's operator ``*`` with ``failOnError`` as true.

.. function:: checked_subtract(x, y) -> [same as x]

    Returns the result of subtracting y from x. The types of x and y must be the same.
    For integral types, overflow results in an error. Corresponds to Spark's operator ``-`` with ``failOnError`` as true.

.. spark:function:: cos(x) -> double

    Returns the cosine of ``x``.

.. spark:function:: cosh(x) -> double

    Returns the hyperbolic cosine of ``x``.

.. spark:function:: cot(x) -> double

    Returns the cotangent of ``x``(measured in radians). Supported type is DOUBLE.

.. spark:function:: csc(x) -> double

    Returns the cosecant of ``x``.

.. spark:function:: degrees(x) -> double

    Converts angle x in radians to degrees.

.. spark:function:: divide(x, y) -> double

    Returns the results of dividing x by y. Performs floating point division.
    Supported type is DOUBLE.
    Corresponds to Spark's operator ``/``. ::

        SELECT 3 / 2; -- 1.5
        SELECT 2L / 2L; -- 1.0
        SELECT 3 / 0; -- NULL

.. spark:function:: divide(x, y) -> decimal

    Returns the results of dividing x by y.
    Supported type is DECIMAL which can be different precision and scale.
    Performs floating point division.
    The result type depends on the precision and scale of x and y.
    Overflow results return null. Corresponds to Spark's operator ``/``. ::

        SELECT CAST(1 as DECIMAL(17, 3)) / CAST(2 as DECIMAL(17, 3)); -- decimal 0.500000000000000000000
        SELECT CAST(1 as DECIMAL(20, 3)) / CAST(20 as DECIMAL(20, 2)); -- decimal 0.0500000000000000000
        SELECT CAST(1 as DECIMAL(20, 3)) / CAST(0 as DECIMAL(20, 3)); -- NULL

.. spark:function:: exp(x) -> double

    Returns Euler's number raised to the power of ``x``.

.. spark:function:: expm1(x) -> double

    Returns Euler's number raised to the power of ``x``, minus 1, which is ``exp(x) - 1`` in math. This function expm1(x) is more accurate than ``exp(x) - 1``, when ``x`` is close to zero.
    If the argument is NaN, the result is NaN.
    If the argument is positive infinity, then the result is positive infinity.
    If the argument is negative infinity, then the result is -1.0.
    If the argument is zero, then the result is a zero with the same sign as the argument.

.. spark:function:: factorial(x) -> bigint

    Returns the factorial of integer ``x``, defined as ``x! = x * (x-1) * ... * 1``.
    The input must be between 0 and 20 inclusive. Returns NULL for values outside this range.

    ::

        SELECT factorial(0); -- 1
        SELECT factorial(5); -- 120
        SELECT factorial(20); -- 2432902008176640000
        SELECT factorial(21); -- NULL
        SELECT factorial(-1); -- NULL

.. spark:function:: floor(x) -> [same as x]

    Returns ``x`` rounded down to the nearest integer.
    Supported types are: BIGINT,  DOUBLE and DECIMAL.

.. spark:function:: hex(x) -> varchar

    Converts ``x`` to hexadecimal.
    Supported types are: BIGINT, VARBINARY and VARCHAR.
    If the argument is a VARCHAR or VARBINARY, the result is string where each input byte is represented using 2 hex characters.
    If the argument is a positive BIGINT, the result is a hex representation of the number (up to 16 characters),
    if the argument is a negative BIGINT, the result is a hex representation of the number which will be treated as two's complement. ::

        SELECT hex("Spark SQL"); -- 537061726B2053514C
        SELECT hex(17); -- 11
        SELECT hex(-1); -- FFFFFFFFFFFFFFFF


.. spark:function:: hypot(a, b) -> double

    Returns the square root of `a` squared plus `b` squared.

.. spark:function:: isnan(x) -> boolean

    Returns true if x is Nan, or false otherwise. Returns false is x is NULL.
    Supported types are: REAL, DOUBLE.

.. spark:function:: log(base, expr) -> double

    Returns the logarithm of ``expr`` with ``base``.
    Returns NULL if either ``expr`` or ``base`` is less than or equal to 0.

.. spark:function:: log1p(x) -> double

    Returns the natural logarithm of the “given value ``x`` plus one”.
    Returns NULL if x is less than or equal to -1.

.. spark:function:: log2(x) -> double

    Returns the logarithm of ``x`` with base 2. Return null for zero and non-positive input.

.. spark:function:: log10(x) -> double

    Returns the logarithm of ``x`` with base 10. Return null for zero and non-positive input.

.. spark:function:: sqrt(x) -> double

    Returns the square root of ``x``.

.. spark:function:: multiply(x, y) -> [same as x]

    Returns the result of multiplying x by y. The types of x and y must be the same.
    Corresponds to Spark's operator ``*``.

.. spark:function:: multiply(x, y) -> [decimal]

    Returns the result of multiplying x by y. The types of x and y must be decimal which can be different precision and scale.
    The result type depends on the precision and scale of x and y.
    Overflow results return null. Corresponds to Spark's operator ``*``. ::

        SELECT CAST(1 as DECIMAL(17, 3)) * CAST(2 as DECIMAL(17, 3)); -- decimal 2.000000
        SELECT CAST(1 as DECIMAL(20, 3)) * CAST(20 as DECIMAL(20, 2)); -- decimal 20.00000
        SELECT CAST(1 as DECIMAL(20, 3)) * CAST(0 as DECIMAL(20, 3)); -- decimal 0.000000
        SELECT CAST(201e-38 as DECIMAL(38, 38)) * CAST(301e-38 as DECIMAL(38, 38)); -- decimal 0.0000000000000000000000000000000000000

.. spark:function:: not(x) -> boolean

    Logical not. ::

        SELECT not true; -- false
        SELECT not false; -- true
        SELECT not NULL; -- NULL

.. spark:function:: pmod(n, m) -> [same as n]

    Returns the positive remainder of n divided by m.
    Supported types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL and DOUBLE.

.. spark:function:: power(x, p) -> double

    Returns ``x`` raised to the power of ``p``.

.. spark:function:: rand() -> double

    Returns a random value with uniformly distributed values in [0, 1). ::

        SELECT rand(); -- 0.9629742951434543

.. spark:function:: rand(seed) -> double

    Returns a random value with uniformly distributed values in [0, 1) using a seed formed
    by combining user-specified ``seed`` and the configuration `spark.partition_id`. The
    framework is responsible for deterministic partitioning of the data and assigning unique
    `spark.partition_id` to each thread (in a deterministic way) .
    ``seed`` must be constant. NULL ``seed`` is identical to zero ``seed``. ::

        SELECT rand(0);    -- 0.5488135024422883
        SELECT rand(NULL); -- 0.5488135024422883

.. spark:function:: random() -> double

    An alias for ``rand()``.

.. spark:function:: random(seed) -> double

    An alias for ``rand(seed)``.

.. spark:function:: remainder(n, m) -> [same as n]

    Returns the modulus (remainder) of ``n`` divided by ``m``. Corresponds to Spark's operator ``%``.
    Supported types are: TINYINT, SMALLINT, INTEGER, BIGINT, REAL and DOUBLE.

.. spark:function:: rint(x) -> double

    Returns the double value that is closest in value to the argument and is
    equal to a mathematical integer.
    Returns ``x`` if ``x`` is a positive or negative infinity or a NaN. ::

        SELECT rint(12.3456); -- 12.0

.. spark:function:: round(x, d) -> [same as x]

    Returns ``x`` rounded to ``d`` decimal places using HALF_UP rounding mode.
    In HALF_UP rounding, the digit 5 is rounded up.
    Supported types for ``x`` are integral and floating point types.

.. spark:function:: sec(x) -> double

    Returns the secant of ``x``.

.. spark:function:: sign(x) -> double

    Returns the signum of ``x``. Supported type for ``x`` is DOUBLE.
    It returns:

    * 0.0 if the argument is 0.0,
    * 1.0 if the argument is greater than 0.0,
    * -1.0 if the argument is less than 0.0,
    * NaN if the argument is NaN,
    * 1.0 if the argument is +Infinity,
    * -1.0 if the argument is -Infinity.

.. spark:function:: sinh(x) -> double

    Returns hyperbolic sine of ``x``.

.. spark:function:: subtract(x, y) -> [same as x]

    Returns the result of subtracting y from x. The types of x and y must be the same.
    Corresponds to Spark's operator ``-``.

.. spark:function:: subtract(x, y) -> decimal

    Returns the result of subtracting ``y`` from ``x``. Reuses the logic of add function for decimal type.
    Corresponds to Spark's operator ``-``.

    ::

        SELECT CAST(1.1232100 as DECIMAL(38, 7)) - CAST(1 as DECIMAL(10, 0)); -- DECIMAL(38, 6) 0.123210
        SELECT CAST(-999999999999999999999999999.999 as DECIMAL(30, 3)) - CAST(-999999999999999999999999999.999 as DECIMAL(30, 3)); -- DECIMAL(31, 3) 0.000
        SELECT CAST(99999999999999999999999999999999.99998 as DECIMAL(38, 6)) - CAST(-0.00001 as DECIMAL(38, 5)); -- DECIMAL(38, 6) 99999999999999999999999999999999.999990
        SELECT CAST(-99999999999999999999999999999999990.0 as DECIMAL(38, 3)) - CAST(0.00001 as DECIMAL(38, 7)); -- DECIMAL(38, 6) NULL

.. spark:function:: unaryminus(x) -> [same as x]

    Returns the negative of `x`.  Corresponds to Spark's operator ``-``.

.. spark:function:: unhex(x) -> varbinary

    Converts hexadecimal varchar ``x`` to varbinary.
    ``x`` is considered case insensitive and expected to contain only hexadecimal characters 0-9 and A-F.
    If ``x`` contains non-hexadecimal character, the function returns NULL.
    When ``x`` contains an even number of characters, each pair is converted to a single byte. The number of bytes in the result is half the number of bytes in the input.
    When ``x`` contains an odd number of characters, the first character is decoded into the first byte of the result and the remaining pairs of characters are decoded into subsequent bytes. This behavior matches Spark 3.3.2 and newer. ::

        SELECT unhex("23"); -- #
        SELECT unhex("f"); -- \x0F
        SELECT unhex("b2323"); -- \x0B##
        SELECT unhex("G"); -- NULL
        SELECT unhex("G23"); -- NULL

.. spark:function:: width_bucket(x, bound1, bound2, n) -> bigint

    Returns the zero-based bucket number to which ``x`` would be assigned in an equiwidth histogram with ``n`` buckets,
    in the range ``bound1`` to ``bound2``.
    `bound1` can be greater than `bound2`.
    If `bound1` less than `bound2`, if `x` less than `bound1` return 0, if `x` greater than or equal to `bound2` return n + 1.
    If `bound1` greater than `bound2`, if `x` greater than `bound1` return 0, if `x` less than or equal to `bound2` return n + 1.
    `n` must be a positive integral value. `x`, `bound1`, and `bound2` cannot be NaN. `bound1`, and `bound2` must be finite.
    `bound1` cannot equal `bound2`;
    Otherwise, the function will return NULL.

    ::

        SELECT width_bucket(-1.0, 0.0, 10.0, 5); -- 0
        SELECT width_bucket(0.1, 0.0, 10.0, 5); -- 1
        SELECT width_bucket(10.1, 0.0, 10.0, 5); -- 6
        SELECT width_bucket(-1.0, 10.0, 0.0, 5); -- 6
        SELECT width_bucket(0.1, 10.0, 0.0, 5); -- 5
        SELECT width_bucket(10.1, 10.0, 0.0, 5); -- 0
        SELECT width_bucket(10.1, 10.0, 10.0, 5); -- NULL
