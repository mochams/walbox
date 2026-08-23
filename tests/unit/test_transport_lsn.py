import pytest

from walbox.transport import _format_lsn
from walbox.transport import _parse_lsn


def test_format_lsn_renders_hex_slash_form():
    assert _format_lsn(0x16B374D848) == "16/B374D848"


def test_format_lsn_zero():
    assert _format_lsn(0) == "0/0"


@pytest.mark.parametrize(
    "lsn",
    [0, 1, 0x16B374D848, 2**63, 2**63 + 12345],
)
def test_parse_lsn_is_the_inverse_of_format_lsn(lsn):
    assert _parse_lsn(_format_lsn(lsn)) == lsn
