import random
import string
import datetime

def generate_random_date(min_year, max_year):
    year = random.randint(min_year, max_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return datetime.datetime(year, month, day, hour, minute, second)

def generateLogUserBehavior() -> dict[str, any]:
    actions = ["click", "hover", "scroll", "drag"]
    components = ["button", "input", "table", "form"]
    stimuli = ["User clicked on a button", "User hovered over an input field",
               "User scrolled through a table", "User dragged a form element"]

    action = random.choice(actions)
    user_author_id = random.randint(1, 1000)
    stimulus = random.choice(stimuli)
    component = random.choice(components)
    text_content = ''.join(random.choices(string.ascii_letters + string.digits, k=50))
    date = generate_random_date(2022, 2023)
    buttonProductId = random.randint(1, 7) if action == "click" else 0
    
    return {"user_author_id": user_author_id, "action": action, "button_product_id": buttonProductId, "stimulus": stimulus, "component": component, "text_content": text_content, "date": date}