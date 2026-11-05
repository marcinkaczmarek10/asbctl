from asbctl.exceptions import AsbctlError, BinaryNotFoundError


def test_asbctl_error_carries_the_code_message_and_exit_code():
    # Given
    code, message, exit_code = "partial", "2 of 9 were not sent", 3

    # When
    error = AsbctlError(code, message, exit_code)

    # Then
    assert (error.code, error.message, error.exit_code) == (code, message, exit_code)
    assert str(error) == "partial: 2 of 9 were not sent (exit 3)"


def test_binary_not_found_error_is_an_exception_of_its_own():
    # Given
    message = "set ASBCTL_BIN"

    # When
    error = BinaryNotFoundError(message)

    # Then
    assert not isinstance(error, AsbctlError)
    assert str(error) == message
