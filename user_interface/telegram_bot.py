from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher.filters import Command
import asyncio
from config import TELEGRAM_API_TOKEN, TELEGRAM_CHAT_IDS

class TelegramBot():
    def __init__(self, strategy, token, chat_ids):
        self.bot = Bot(token=token)
        self.dp = Dispatcher(self.bot)
        self.strategy = strategy
        self.my_chats = chat_ids

        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['start'])(self.cmd_start)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['grid'])(self.cmd_grid)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['limit_grid'])(self.cmd_limit_grid)        
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['stop'])(self.cmd_stop)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['set_freq'])(self.cmd_set_freq)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['set_risk'])(self.cmd_set_risk)
        self.dp.message_handler(UserIdFilter(self.my_chats))(self.echo_message)



    async def cmd_start(self, message: types.Message):
        """
        Conversation's entry point
        """
        await message.answer("Hi there! I'm your trading bot.")

    async def cmd_grid(self, message: types.Message):
        text_parts = message.text.split()
        if len(text_parts) < 7:
            await message.reply("Please provide all required arguments: /grid symbol spread_step pose_step number_of_steps mid_spread mid_pose")
            return

        symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose = text_parts[1:7]
        spread_step, pose_step, number_of_steps, mid_spread, mid_pose = float(spread_step), float(pose_step), int(number_of_steps), float(mid_spread), float(mid_pose)
        await message.reply(f"Started grid trading for contract {symbol}")
        await self.strategy.start_grid_trading(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)


    async def cmd_limit_grid(self, message: types.Message):
        text_parts = message.text.split()
        if len(text_parts) < 7:
            await message.reply("Please provide all required arguments: /limit_grid symbol spread_step pose_step number_of_steps mid_spread mid_pose")
            return

        symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose = text_parts[1:7]
        spread_step, pose_step, number_of_steps, mid_spread, mid_pose = float(spread_step), float(pose_step), int(number_of_steps), float(mid_spread), float(mid_pose)
        await message.reply(f"Started limit_grid trading for contract {symbol}")
        await self.strategy.start_limit_trading(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)



    async def cmd_stop(self, message: types.Message):
               
        text_parts = message.text.split()
        if len(text_parts) > 1:
            symbol = text_parts[1]
            await self.strategy.stop_trading(symbol)
            await message.reply(f"Stopped subscription for contract {symbol}")
        else:
            await self.strategy.stop_trading()
            await message.reply("Stopped subscriptions for all contracts")


    async def cmd_set_freq(self, message: types.Message):
        """
        Command to set threshold in the mean reversion strategy
        """
        _, new_timeout = message.text.split()
        new_timeout = float(new_timeout)
        self.strategy.timeout = new_timeout
        await message.answer(f"New timeoute set to: {new_timeout}")

    async def cmd_set_risk(self, message: types.Message):
        """
        Command to set threshold in the mean reversion strategy
        """
        _, new_risk_coef = message.text.split()
        new_risk_coef = float(new_risk_coef)
        self.strategy.risk_coef = new_risk_coef
        for symbol in self.strategy.symbols:
            self.strategy.max_risk[symbol] = self.strategy.moex_connector.lots_dict[symbol] * self.strategy.risk_coef
        await message.answer(f"New risk_coef set to: {new_risk_coef}")

    async def echo_message(self, msg: types.Message):
        await self.bot.send_message(msg.from_user.id, msg.text)

# replace (?) send_message with a custom messages to pre-specified chat_ids
    async def send_message(self, message):
        for chat_id in self.my_chats:
            await self.bot.send_message(chat_id, message)

    async def run(self):
        self.strategy.set_bot(self)
        await self.dp.start_polling()

# class for checking the chat_id of incoming nessages. NOT TESTED TODO: TEST
class UserIdFilter(Command):
    def __init__(self, user_ids):
        self.user_ids = user_ids

    async def check(self, message: types.Message):
        return message.chat.id in self.user_ids