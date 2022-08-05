#!/usr/bin/env python
# coding: utf-8

# In[13]:


import asyncio  # импортируем библиотеку для работы асинхронной функции
from datetime import datetime  # Модуль для еукущей даты и времени

import psycopg2  # Модуль для подколючения к PostgreSQL
from binance import Client  # Модуль для раьботы с Бинанс
from psycopg2 import Error

api_key = '' #введите свои ключ
api_secret = '' #введите свой ключ
""" Соединение с Бинансом через API"""
client = Client(api_key, api_secret)
client.API_URL = 'https://testnet.binance.vision/api' #это для работы в тестовом аккаунте. для работы в реальной бирже это нужно убрать
info = client.get_account()

"""Задаем базовые глобальные переменные"""
BALBNB = 0
BALBUSD = 0
orderID = 0
stat = False  # Для проверки размещения ордера
actprice = 0


def DBlaspopprices(OT):  # Запрос последних цен опрераций при старте программы в БД на PostgreSQL/ нужно зоздать свою БД
    global LSPrise, LBPrice
    connect = None
    try:
        connect = psycopg2.connect(
            database='Tradebot',# имя БД
            user='postgres',# ваше имя пользователя, указано стандартное
            password='',# ваш пароль
            host='127.0.0.1',# хост
            port='5432')# порт
        cursor = connect.cursor()
        print('Соединение с базой данных для обновления LSPrice and LBPrise успешно установлено')
        cursor.execute(
            """SELECT "Price" FROM "TradeOperations" WHERE "Status" = 'FILLED' AND "Type" = %s ORDER BY "DateTime" DESC""",
            (OT,))
        results = cursor.fetchone()
        result = float(results[0])
        print(result)
        return result
    except (Exception, Error) as e:
        print('Ошибка ', e)
    finally:
        if connect:
            cursor.close()
            connect.close()
            print("Соединение с PostgreSQL закрыто.LSPrice and LBPrise получены")

LBprice = DBlaspopprices('Buy')#цена последней покупки
LSprice = DBlaspopprices('Sell') #цена последней продажи

# Получение данных о цене по выбранной торговой паре
async def Getprice():
    global actprice
    a = client.get_symbol_ticker(symbol="BNBBUSD")
    actprice = float(a['price'])
    return (actprice)


# Получение данных о балансе по выбранной валюте
async def Getbal():
    global BALBNB, BALBUSD
    bal = info['balances']
    for b in bal:
        if b['asset'] == 'BNB':
            BALBNB = float(b['free'])
        elif b['asset'] == 'BUSD':
            BALBUSD = float(b['free'])
            return (BALBNB, BALBUSD)


# Создание лимитного ордера на продажу по запрошенной рыночной цене
async def Sellorder(actprice):
    global stat
    stat = False  # обнуление статуса ордера
    try:
        order = client.order_limit_sell(
            symbol='BNBBUSD',
            quantity=0.1,
            price=actprice)
        stat = True  # Отметка о том, что ордер размещен, для запуска функции выгрузки в БД
        return (stat)
    except:
        print("Не удалось разместить ордер на продажу")


# Создание лимитного ордера на покупку по запрошенной рыночной цене
async def Buyorder(actprice):
    global stat
    stat = False
    try:
        order = client.order_limit_buy(
            symbol='BNBBUSD',
            quantity=0.1,
            price=actprice)
        stat = True  # Отметка о том, что ордер размещен, для запуска функции выгрузки в БД
        return stat
    except:
        print("Не удалось разместить ордер на покупку")


# Получение ID ордера
async def MyorderId():
    global orderID
    myorder = client.get_all_orders(symbol="BNBBUSD", limit=1)
    for m in myorder:
        orderID = m['orderId']
    return orderID


# Проверка статуса исполнения ордера
async def OrderStat(Id):
    global orderID,order_stat
    stat = client.get_order(
        orderId=Id,
        symbol='BNBBUSD')
    order_stat = stat['status']
    return (stat['status'])


# Функция добавления информации в БД
async def DBaddTO(DBname, userId, userPs, DBHost, DBport, opertype):
    global BALBNB, BALBUSD, LBprice, LSprice, stat, orderID, actprice
    await MyorderId()
    connect = None
    try:
        connect = psycopg2.connect(
            database=DBname,
            user=userId,
            password=userPs,
            host=DBHost,
            port=DBport)
        cursor = connect.cursor()
        print('Соединение с базой данных успешно установлено для добавления операции')
        print('OrderID=', orderID)
        cursor.execute(
            'Insert into "TradeOperations" ("OperId", "Type", "DateTime", "Status", "Quantity", "Price") Values (%s, %s, %s, %s, %s, %s)',
            (orderID, opertype, datetime.now(), 'NEW', 0.05, actprice))  # Добавление записи об операции в БД
        cursor.execute('Insert into "Balance" ("OperId", "Datetime", "BNB", "BUSD") Values (%s, %s, %s, %s)',
                       (orderID, datetime.now(), BALBNB, BALBUSD))  # Добавление записи о ,балансе в БД
        connect.commit()
    except (Exception, Error) as e:
        print('Ошибка ', e)
    finally:  # Закрытие соединяния. Пока не знаю куда, но точно где-то должно быть
        if connect:
            cursor.close()
            connect.close()
            print("Соединение с PostgreSQL закрыто. Операция сохранена")
    return connect


async def DBstatupdate():  # Провека статуса ордеров и обновление последней цены
    global LSPrise, LBPrice, order_stat
    while True:
        connect = None
        try:
            connect = psycopg2.connect(
                database='Tradebot',
                user='postgres',
                password='Nas22011991',
                host='127.0.0.1',
                port='5432')
            cursor = connect.cursor()
            print('Соединение с базой данных для обновления статуса успешно установлено')
            cursor.execute("""SELECT * FROM "TradeOperations" WHERE "Status" = 'NEW' ORDER BY "DateTime" DESC""")
            results = cursor.fetchall()
            for result in results:  # проверяем все открытые ордера
                order_stat = None
                await OrderStat(str(result[0]))
                if order_stat == 'FILLED':
                    print(result, "Is FILLED")
                    cursor.execute("""UPDATE "TradeOperations" SET "Status" = 'FILLED' WHERE "OperId" = %s;""",
                                   (result[0],))  # обновляем статус по закрывшимся ордерам в БД
                    connect.commit()
                    if result[1] == 'Sell':  # обновляем последнюю цену продажи/покупки для дальнейшей работы бота
                        LSPrice = result[5]
                    else:
                        LBPrice = result[5]
        except (Exception, Error) as e:
            print('Ошибка ', e)
        finally:
            if connect:
                cursor.close()
                connect.close()
                print("Соединение с PostgreSQL закрыто.Статус обновлен")
        await asyncio.sleep(60)


# Прописываем главный цикл работы бота
async def trade_operations():
    global BALBNB, BALBUSD, LBprice, LSprice, stat, orderID, actprice
    while True:
        await Getprice()
        await Getbal()
        print('actprice= ', actprice)  # !!!
        print("BALBNB =", BALBNB)  # !!!
        print("BALBUSD=", BALBUSD)  # !!!
        if actprice < LSprice * 0.98 and BALBUSD >= actprice * 0.12:
            print('пробую разместить ордер на продажу')  # !!!
            await Buyorder(actprice)
            print(stat)  # !!!
            if stat == True:
                print('buy')  # !!!
                await DBaddTO('Tradebot', 'postgres', 'ПАРОЛЬ', "127.0.0.1", "5432", "Buy") # здесь нужно прописать ваши данные учетной записи БД
            elif stat == False:
                print('ордер не размещен')
        elif actprice > LBprice * 1.02 and BALBNB >= 0.1:
            await Sellorder(actprice)
            if stat == True:
                print('sell')
                await DBaddTO('Tradebot', 'postgres', 'ПАРОЛЬ', "127.0.0.1", "5432", "Sell") # здесь нужно прописать ваши данные учетной записи БД
            else:
                print('ордер не размещен')
        else:
            print('Нет подходящих операций')
        await asyncio.sleep(300)

async def main():
    main_loop.create_task(DBstatupdate())
    main_loop.create_task(trade_operations())

"""Задаем функцию асинхронного цикла"""
main_loop = asyncio.new_event_loop()
main_loop.run_until_complete(main())
main_loop.run_forever()