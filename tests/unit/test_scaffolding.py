import asyncio


def test_pytest_discovers_unit_tests():
    assert True


async def test_asyncio_mode_is_active():
    sentinel = object()
    await asyncio.sleep(0)
    result = sentinel
    assert result is sentinel
