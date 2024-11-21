class SagaStep:
    def __init__(self, action, compensation=None):
        self.action = action
        self.compensation = compensation
        self.state = {}

    def execute(self, *args, **kwargs):
        try:
            self.state = self.action(*args, **kwargs)
            return True
        except Exception as e:
            return False

    def compensate(self):
        if self.compensation and self.state:
            try:
                self.compensation(self.state)
            except Exception as e:
                # Логируем ошибку компенсации, но продолжаем
                pass


class Saga:
    def __init__(self):
        self.steps = []
        self.current_step = -1

    def add_step(self, action, compensation=None):
        self.steps.append(SagaStep(action, compensation))

    def execute(self):
        try:
            # Выполняем все шаги
            for i, step in enumerate(self.steps):
                self.current_step = i
                if not step.execute():
                    # Если шаг не удался, запускаем компенсацию
                    self._compensate()
                    return False
            return True
        except Exception as e:
            self._compensate()
            return False

    def _compensate(self):
        # Компенсируем все выполненные шаги в обратном порядке
        for i in range(self.current_step, -1, -1):
            self.steps[i].compensate()
