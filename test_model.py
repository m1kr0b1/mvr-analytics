#!/usr/bin/env python3
import asyncio
from openai import AsyncOpenAI
from config import get_settings

async def test():
    settings = get_settings()
    print('Testing model:', settings.llm_model)
    
    client = AsyncOpenAI(api_key=settings.openrouter_api_key, base_url=settings.openrouter_base_url)
    
    test_text = '''Надворешната канцеларија за криминалистички работи Свети Николе поднесе кривична пријава против Д.С.(36) од Свети Николе поради постоење основи на сомнение за сторено кривично дело „неовластено производство и пуштање во промет на наркотични дроги, психотропни супстанци и прекурзори". На 04.04.2026, полициски службеници извршиле претрес во домот на пријавениот.'''
    
    try:
        response = await client.chat.completions.create(
            model=settings.llm_model,
            messages=[
                {'role': 'system', 'content': 'You extract crime incidents from text. Return JSON array.'},
                {'role': 'user', 'content': f'Extract crime incidents from this text:\n\n{test_text}'}
            ],
            max_tokens=500,
            temperature=0.1
        )
        content = response.choices[0].message.content
        print('Response:', content)
    except Exception as e:
        print('Error:', type(e).__name__, str(e)[:300])
    finally:
        await client.close()

asyncio.run(test())