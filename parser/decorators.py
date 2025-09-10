import functools
import logging
import time
from datetime import datetime as dt
from parser.logging_config import setup_logging


setup_logging()


def time_of_function(func):
    """
    Декоратор для измерения времени выполнения функции.

    Замеряет время выполнения декорируемой функции и логирует результат
    в секундах и минутах. Время округляется до 3 знаков после запятой
    для секунд и до 2 знаков для минут.

    Args:
        func (callable): Декорируемая функция, время выполнения которой
        нужно измерить.

    Returns:
        callable: Обёрнутая функция с добавленной функциональностью
        замера времени.
    """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = round(time.time() - start_time, 3)
        logging.info(
            f'Функция {func.__name__} завершила работу. '
            f'Время выполнения - {execution_time} сек. '
            f'или {round(execution_time / 60, 2)} мин.'
        )
        return result
    return wrapper


def time_of_script(func):
    """Декортаор для измерения времени работы всего приложения."""
    @functools.wraps(func)
    def wrapper():
        date_str = dt.now().strftime('%Y-%m-%d')
        time_str = dt.now().strftime('%H:%M:%S')
        run_id = str(int(time.time()))
        print(f'Функция main начала работу {date_str} в {time_str}')
        start_time = time.time()
        try:
            result = func()
            execution_time = round(time.time() - start_time, 3)
            print(
                'Функция main завершила '
                f'работу в {dt.now().strftime("%H:%M:%S")}.'
                f' Время выполнения - {execution_time} сек. '
                f'или {round(execution_time / 60, 2)} мин.'
            )
            logging.info('SCRIPT_FINISHED_STATUS=SUCCESS')
            logging.info(f'DATE={date_str}')
            logging.info(f'EXECUTION_TIME={execution_time} сек')
            logging.info(f'FUNCTION_NAME={func.__name__}')
            logging.info(f'RUN_ID={run_id}')
            logging.info('ENDLOGGING=1')
            return result
        except Exception as e:
            execution_time = round(time.time() - start_time, 3)
            print(
                'Функция main завершилась '
                f'с ошибкой в {dt.now().strftime("%H:%M:%S")}. '
                f'Время выполнения - {execution_time} сек. '
                f'Ошибка: {e}'
            )
            logging.info('SCRIPT_FINISHED_STATUS=ERROR')
            logging.info(f'DATE={date_str}')
            logging.info(f'EXECUTION_TIME={execution_time} сек')
            logging.info(f'ERROR_TYPE={type(e).__name__}')
            logging.info(f'ERROR_MESSAGE={str(e)}')
            logging.info(f'FUNCTION_NAME={func.__name__}')
            logging.info(f'RUN_ID={run_id}')
            logging.info('ENDLOGGING=1')
            raise
    return wrapper
