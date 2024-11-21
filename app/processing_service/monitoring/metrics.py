class MetricsCollector:
    def __init__(self):
        self.metrics = {}
        
    def increment(self, metric_name: str, value: int = 1):
        if metric_name not in self.metrics:
            self.metrics[metric_name] = 0
        self.metrics[metric_name] += value
        
    def observe(self, metric_name: str, value: float):
        self.metrics[metric_name] = value
