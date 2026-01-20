# pipeline/dq_framework/exceptions.py

class DataQualityError(Exception):
    def __init__(self, dataset_name: str, errors: dict):
        self.dataset_name = dataset_name
        self.errors = errors
        super().__init__(self._format())

    def _format(self) -> str:
        lines = [f"Data quality checks failed for dataset '{self.dataset_name}':"]
        for k, v in self.errors.items():
            lines.append(f"- {k}: {v}")
        return "\n".join(lines)
