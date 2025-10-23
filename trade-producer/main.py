import asyncio

from src.trade_producer import BitfinexCandleSource


def main():
    asyncio.run(run())


async def run():
    source = BitfinexCandleSource(symbol="tBTCUSD", timeframe="1m")
    await source.run()


if __name__ == "__main__":
    main()
