""" Ejemplo de uso del paquete teii. """


import logging
import matplotlib.pyplot as plt
import datetime as dt
import teii.finance as tf


def setup_logging(logging_level):
    """ Crea y configura logger. """

    # TODO: Tarea 1
    #   Configura logging para enviar la salida a un archivo

    logging.basicConfig(filename='example.log',format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging_level)
    logger.info("Logger creado")

    return logger


def plot(pandas_series, ticker, logger):
    """ Dibuja una gráfica a partir de la serie de Pandas. """

    logger.info("Dibujando gráfica...")

    pandas_series.plot(xlabel='Fecha', ylabel='Precio en USD', title=f"Evolución del Precio de {ticker}")
    plt.show()  # ¡Necesario para que se muestre la gráfica en una ventana!


def main():
    """ Muestra como usar teii-finance. """

    logger = setup_logging(logging.INFO)

    logger.info("Inicio")

    # Define ticker y API key
    ticker = ['AMZN',]
    my_alpha_vantage_api_key = 'https://www.alphavantage.co/support/#api-key'

    # Crea cliente
    try:
        tf_client = tf.TimeSeriesFinanceClient(ticker,
                                               my_alpha_vantage_api_key,
                                               logging_level=logging.INFO)
    # Captura y muestra todas las excepciones
    except Exception as e:
        logger.error(f"{e}", exc_info=False)
    # Usa el cliente
    else:
        # TODO: Tarea 3
        #   Filtra los datos para mostrar únicamente el año 2020

        # Genera una serie de Pandas con precio de cierre diario
        from_date = dt.date(year = 2020, month = 1, day = 1)
        to_date = dt.date(year = 2020, month = 12, day = 31)
        pd_series = tf_client.daily_price(from_date, to_date)

        logger.info(pd_series)

        # Dibuja una gráfica a partir de la serie de Pandas
        plot(pd_series[0], ticker, logger)
    finally:
        logger.info("Fin")


if __name__ == "__main__":
    main()
