from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher.filters import Command
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.types import ReplyKeyboardRemove, \
    ReplyKeyboardMarkup, KeyboardButton, \
    InlineKeyboardMarkup, InlineKeyboardButton
import numpy as np
import asyncio
from config import TELEGRAM_API_TOKEN, TELEGRAM_CHAT_IDS

class LimitGridStates(StatesGroup):
    waiting_for_confirmation = State()  # Define a state


class TelegramBot():
    def __init__(self, strategy, token, chat_ids):
        self.bot = Bot(token=token)

        self.storage = MemoryStorage()
        self.dp = Dispatcher(self.bot, storage = self.storage)
        self.strategy = strategy
        self.my_chats = chat_ids

        # used to save parameters of latest strategy

        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['start'])(self.cmd_start)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['grid'])(self.cmd_grid)
        self.dp.message_handler(UserIdFilter(self.my_chats), state='*', commands=['limit_grid'])(self.cmd_limit_grid)        
        self.dp.message_handler(UserIdFilter(self.my_chats), state='*', commands=['stop'])(self.cmd_stop)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['set_freq'])(self.cmd_set_freq)
        self.dp.message_handler(UserIdFilter(self.my_chats), commands=['set_risk'])(self.cmd_set_risk)
        self.dp.message_handler(UserIdFilter(self.my_chats))(self.echo_message)

        self.dp.callback_query_handler(text = "confirm_limit_grid", state=LimitGridStates.waiting_for_confirmation)(self.confirm_command)
        self.dp.callback_query_handler(text = "decline_limit_grid", state=LimitGridStates.waiting_for_confirmation)(self.decline_command)



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


    async def cmd_limit_grid(self, message: types.Message, state: FSMContext):
        text_parts = message.text.split()
        if len(text_parts) < 7:
            await message.reply("Please provide all required arguments: /limit_grid symbol spread_step pose_step number_of_steps mid_spread mid_pose")
            return

        symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose = text_parts[1:7]
        spread_step, pose_step, number_of_steps, mid_spread, mid_pose = float(spread_step), float(pose_step), int(number_of_steps), float(mid_spread), float(mid_pose)
        params = {
            "symbol": symbol,
            "spread_step": spread_step,
            "pose_step": pose_step,
            "number_of_steps": number_of_steps,
            "mid_spread": mid_spread,
            "mid_pose": mid_pose,
        }
                
        grid = [(i-(number_of_steps - 1)/2) for i in range(number_of_steps)]
        spread_grid = np.around([spread_step * i + mid_spread for i in grid],3)
        pose_grid = np.around([pose_step * i + mid_pose for i in grid],0)

        await state.set_data(params)
        await state.set_state(LimitGridStates.waiting_for_confirmation)

        markup = InlineKeyboardMarkup()
        markup.add(
            InlineKeyboardButton("Confirm", callback_data="confirm_limit_grid"),
            InlineKeyboardButton("Decline", callback_data="decline_limit_grid")
        )

        await message.reply(f"Received following trading grids for {symbol}:\nspread_grid = {str(spread_grid)}\n{symbol} pose_grid = {str(pose_grid)}\nPlease confirm:", reply_markup=markup)
        #await self.strategy.start_limit_trading(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)



    async def confirm_command(self, callback_query: types.CallbackQuery, state: FSMContext):
        if callback_query.data == "confirm_limit_grid":
            user_data = await state.get_data()
            # Unpack the parameters from the dictionary
            symbol = user_data["symbol"]
            spread_step = user_data["spread_step"]
            pose_step = user_data["pose_step"]
            number_of_steps = user_data["number_of_steps"]
            mid_spread = user_data["mid_spread"]
            mid_pose = user_data["mid_pose"]

            await callback_query.message.edit_text("Execution of limit_grid command confirmed.")

            # Clear the state
            await state.reset_state()

            await self.strategy.start_limit_trading(symbol, spread_step, pose_step, number_of_steps, mid_spread, mid_pose)

    async def decline_command(self, callback_query: types.CallbackQuery, state: FSMContext):
        if callback_query.data == "decline_limit_grid":
            await callback_query.message.edit_text("Execution of limit_grid command declined.")
            
            # Clear the state
            await state.reset_state()



    async def cmd_stop(self, message: types.Message, state: FSMContext):
        text_parts = message.text.split()
        if len(text_parts) > 1:
            symbol = text_parts[1]
            await self.strategy.stop_trading(symbol)
            await message.reply(f"Stopped subscription for contract {symbol}")
        else:
            await self.strategy.stop_trading()
            await message.reply("Stopped subscriptions for all contracts")

        current_state = await state.get_state()
        if current_state is None:
            return

        print('Cancelling state %r', current_state)
        # Cancel state and inform user about it
        await state.finish()
        # And remove keyboard (just in case)
        await message.reply('State cancelled.', reply_markup=types.ReplyKeyboardRemove())


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
        print("telegram bot run started")
        self.strategy.set_bot(self)
        await self.dp.start_polling()

# class for checking the chat_id of incoming nessages. NOT TESTED TODO: TEST
class UserIdFilter(Command):
    def __init__(self, user_ids):
        self.user_ids = user_ids

    async def check(self, message: types.Message):
        return message.chat.id in self.user_ids