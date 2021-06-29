import asyncio
from binance import AsyncClient, BinanceSocketManager
async def make_americano():
    print("Americano Start")
    await asyncio.sleep(3)
    print("Americano End")

async def make_latte():
    print("Latte Start")
    await asyncio.sleep(5)
    print("Latte End")
# time.sleep 함수가 CPU를 점유하면서 기다리는 것과 달리 asyncio.sleep 함수는 CPU가 다른 코루틴을 처리할 수 있도록 CPU 점유를 해제한 상태로 기다립니다
async def main():
    coro1 = make_americano()
    coro2 = make_latte()
    await asyncio.gather(
        coro1,
        coro2
    )
print("Main Start")
asyncio.run(main())
print("Main End")