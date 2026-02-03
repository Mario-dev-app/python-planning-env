"""
Registro de tareas programables.
Cada tarea tiene un nombre y una función. Se programa desde SCHEDULES en .env
"""
from jobs.registry import registry, register

__all__ = ["registry", "register"]
