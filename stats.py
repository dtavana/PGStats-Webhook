import requests
import discord
from discord import Webhook, RequestsWebhookAdapter, Embed
import config as cfg
import asyncio
import aiomysql
import datetime
import time

# Create webhook
webhook = Webhook.partial(cfg.WEBHOOK_ID, cfg.WEBHOOK_TOKEN,
 adapter=RequestsWebhookAdapter())


async def postStats():
    try:
        dzconn = await aiomysql.connect(host=cfg.dzhost, port=cfg.dzport, user=cfg.dzuser, password=cfg.dzpass, db=cfg.dzschema, autocommit=True)
        dzcur = await dzconn.cursor(aiomysql.DictCursor)
        charSums = {"character_data": [
            "Generation", "KillsH", "KillsB", "DistanceFoot", "HeadshotsZ", "KillsZ"]}
        playerSums = {"player_data": ["BankCoins"]}
        xpSums = {"xpsystem": ["XP"]}
        charAvg = {"character_data": ["Humanity", "Duration"]}
        playerAvg = {"player_data": ["BankCoins"]}
        xpAvg = {"xpsystem": ["XP"]}
        objectCount = {"object_data": [
            "Plastic_Pole_EP1_DZ", "VaultStorageLocked", "*"]}

        tables = {"Totals": [charSums, playerSums, xpSums], "Averages": [
            charAvg, playerAvg, xpAvg], "Counts": [objectCount]}
        sums = {}
        averages = {}
        counts = {}
        realNames = {"Generation": "Total Number of Respawns", "KillsH": "Total Hero Kills", "KillsB": "Total Bandit Kills", "DistanceFoot": "Total Distance Traveled (metres)",
                    "HeadshotsZ": "Total Zombies Killed via Headshot", "KillsZ": "Total Zombies Killed", "Duration": "Total Minutes Survived", "Humanity": "Total Humanity", "BankCoins": "Total Bank Coins", "XP": "Total XP", "Plastic_Pole_EP1_DZ": "Total Number of Plot Poles", "VaultStorageLocked": "Total Number of Safes", "*": "Total Number of Objects"}

        for type in tables:
            for x in tables[type]:
                for key in x:
                    for column in x[key]:
                        if type == "Totals":
                            if column == "XP":
                                query = f"SELECT SUM({column}) AS Total FROM {key} WHERE XP > 100;"
                            else:
                                query = f"SELECT SUM({column}) AS Total FROM {key};"
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Total']
                            sums[column] = result

                        elif type == "Averages":
                            if column == "XP":
                                query = f"SELECT AVG({column}) AS Average FROM {key} WHERE XP > 100;"
                            else:
                                query = f"SELECT AVG({column}) AS Average FROM {key};"
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Average']
                            result = round(result, 0)
                            averages[column] = result

                        elif type == "Counts":
                            if column == "*":
                                query = f"SELECT COUNT({column}) AS Total FROM {key};"
                            else:
                                query = f'SELECT COUNT(IF(Classname = "{column}", 1, NULL)) AS Total FROM {key};'
                            await dzcur.execute(query)
                            result = await asyncio.gather(dzcur.fetchone())
                            result = result[0]['Total']
                            result = round(result, 2)
                            counts[column] = result
        sumStr = ""
        avgStr = ""
        countStr = ""

        for key, value in sums.items():
            trueName = realNames[key]
            sumStr += f"**{trueName}** | **{value}**\n"
        
        for key, value in averages.items():
            trueName = realNames[key]
            trueName = trueName.replace("Total", "Average")
            avgStr += f"**{trueName}** | **{value}**\n"
        
        for key, value in counts.items():
            trueName = realNames[key]
            countStr += f"**{trueName}** | **{value}**\n"

        embed = discord.Embed(
            title=f"Stats at **{time.strftime('%H:%M')} {time.tzname[0]}**", colour=discord.Colour(0x32CD32))
        embed.set_footer(text="PG Stats | TwiSt#7777")
        embed.add_field(name=f"Totals:",
                        value=sumStr, inline=False)
        embed.add_field(name=f"Averages:",
                        value=avgStr, inline=False)
        embed.add_field(name=f"Object Counts:",
                        value=countStr, inline=False)
                
        webhook.send(embed=embed)
    except Exception as e:
        print(e)
    finally:
        dzconn.close()

loop = asyncio.get_event_loop()
loop.run_until_complete(postStats())
