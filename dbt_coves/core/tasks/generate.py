from rich.console import Console

console = Console()


class GenerateTask:
    def run(self) -> int:
        console.print('Source \'test\' generated successfully.')
        return 0
