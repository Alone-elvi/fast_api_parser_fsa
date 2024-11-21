from typing import Dict, Any

class SagaStep:
    def __init__(self, action, compensation=None):
        self.action = action
        self.compensation = compensation
        self.state = {}

    def execute(self, *args, **kwargs):
        try:
            result = self.action(*args, **kwargs)
            self.state = kwargs  # Сохраняем входные данные для компенсации
            return True if result is not None else False
        except Exception as e:
            logger.error(f"Step execution error: {e}")
            return False

    def compensate(self, data: Dict[str, Any]):
        if self.compensation:
            try:
                self.compensation(data)
            except Exception as e:
                logger.error(f"Compensation error: {e}")


class Saga:
    def __init__(self):
        self.steps = []
        self.current_step = -1
        self.completed_steps = []

    def add_step(self, step: SagaStep):
        """Добавляет шаг в сагу"""
        self.steps.append(step)

    def execute(self, data: Dict[str, Any]) -> bool:
        try:
            # Выполняем все шаги
            for i, step in enumerate(self.steps):
                self.current_step = i
                if not step.execute(data):
                    # Если шаг не удался, запускаем компенсацию
                    self._compensate(data)
                    return False
                self.completed_steps.append(i)
            return True
        except Exception as e:
            logger.error(f"Saga execution error: {e}")
            self._compensate(data)
            return False

    def _compensate(self, data: Dict[str, Any]):
        # Компенсируем все выполненные шаги в обратном порядке
        for i in range(self.current_step, -1, -1):
            try:
                self.steps[i].compensate(data)
            except Exception as e:
                logger.error(f"Compensation error in step {i}: {e}")
