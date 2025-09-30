# sms/ai/responder.py
import os
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

class AIResponder:
    @staticmethod
    def reply(context: dict) -> str:
        """
        Generate an AI-powered seller reply based on conversation context.
        Context should include:
        - phone
        - last_message
        - lead fields (Owner Name, Address, Market, etc.)
        """
        prompt = f"""
        You are an AI real estate acquisitions assistant.
        The seller just confirmed ownership & interest in an offer.

        Seller Info:
        {context}

        Your goals:
        1. Thank them for confirming.
        2. Ask politely if they have a number in mind.
        3. Keep it short, natural, and friendly (SMS style).
        4. Never sound robotic or spammy.
        """

        response = client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.6,
        )

        return response.choices[0].message.content.strip()