import psycopg2
import time

import app_logging

logger = app_logging.get_logger()


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время, если возникла ошибка. Использует наивный экспоненциальный рост времени повтора (factor) до граничного времени ожидания (border_sleep_time)

    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """
    def func_wrapper(func):
        def inner(*args, **kwargs):

            n = 0
            t = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except psycopg2.Error as e:
                    t = start_sleep_time * factor ** n if t < border_sleep_time else border_sleep_time
                    logger.error(e)
                    logger.info(f'retry in {t} seconds')
                    time.sleep(t)
                n += 1

        return inner
    return func_wrapper
