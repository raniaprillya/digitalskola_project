import discord
from discord import SyncWebhook


def send_to_discord(webhook_url, output_file,thn):
    """Sending File alerts to discord

    Parameters
    ----------
    webhook_url : string
        _description_
    output_file : string
        _description_
    """

    webhook = SyncWebhook.from_url(webhook_url)

    with open(file=output_file, mode='rb') as file:
        excel_file = discord.File(file)

    webhook.send(f'Automate Sales Report as per {thn}', 
                username='Sales Team', 
                file=excel_file)