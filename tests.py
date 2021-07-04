import unittest

from click.testing import CliRunner
from ya import cli



class TestYewAddress(unittest.TestCase):

    def test_info(self):

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "info",
            ],
        )

        assert result.exit_code == 0


