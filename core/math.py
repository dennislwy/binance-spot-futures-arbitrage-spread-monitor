from decimal import ROUND_HALF_UP, Decimal


def round_dec(value: Decimal, precision: int) -> Decimal:
    """
    Round a Decimal to `precision` decimal places using ROUND_HALF_UP.
    
    Examples:
        round_dec(Decimal("1.23456"), 2) -> Decimal("1.23")
        round_dec(Decimal("1.235"),   2) -> Decimal("1.24")
        round_dec(Decimal("10"),      3) -> Decimal("10.000")
    """
    if not isinstance(value, Decimal):
        raise TypeError("value must be a Decimal")

    # Create the quantizer, e.g. precision=3 -> Decimal("0.001")
    quantizer = Decimal("1").scaleb(-precision)

    return value.quantize(quantizer, rounding=ROUND_HALF_UP)
