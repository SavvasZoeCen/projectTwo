from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def fill_order(order):
    
    """order is a dictionary of below example
    { 
    'buy_currency': "Algorand",
    'sell_currency': "Ethereum", 
    'buy_amount': 1245.00,
    'sell_amount': 2342.31,
    'sender_pk': 'AAAAC3NzaC1lZDI1NTE5AAAAIB8Ht8Z3j6yDWPBHQtOp/R9rjWvfMYo3MSA/K6q8D86r',
    'receiver_pk': '0xd1B77a920A0c5010469F40f14c5e4E03f4357226'
    }
    """
    
    #1.	Insert the order into the database
    session.add(order)
    session.commit()  

    #2.	Check if there are any existing orders that match. 
    orders = session.query(Order).filter(Order.filled == "").all() #Get all unfilled orders
    for existing_order in orders:
      if (existing_order.buy_currency == order.sell_currency and 
        existing_order.sell_currency == order.buy_currency and 
        existing_order.sell_amount/existing_order.buy_amount >= order.buy_amount/order.sell_amount): #match
        print("matched")
    
        #3.	If a match is found between order and existing_order:
        #– Set the filled field to be the current timestamp on both orders
        date_time = datetime.now().strftime("%m/%d/%Y, %H:%M:%S")
        existing_order.filled = date_time
        order.filled = date_time
        
        #– Set counterparty_id to be the id of the other order
        existing_order.counterparty_id = order.id
        order.counterparty_id = existing_order.id

        #– If one of the orders is not completely filled (i.e. the counterparty’s sell_amount is less than buy_amount):
        if existing_order.sell_amount < order.buy_amount: #this order is not completely filled
          parent_order = order
          buy_amount = order.buy_amount - existing_order.sell_amount
          sell_amount = order.sell_amount - existing_order.buy_amount
          print("parent_order = order")
          
        if order.sell_amount < existing_order.buy_amount: #existing_order is not completely filled
          parent_order = existing_order
          buy_amount = existing_order.buy_amount - order.sell_amount
          sell_amount = existing_order.sell_amount - order.buy_amount
          print("parent_order = existing_order")
          
        if parent_order is not None:
          print("parent_order is not None")
          #o	Create a new order for remaining balance
          child_order = {} #new dict
          child_order['buy_amount'] = buy_amount
          child_order['sell_amount'] = sell_amount
          child_order['buy_currency'] = parent_order['buy_currency']
          child_order['sell_currency'] = parent_order['sell_currency']
          
          #o	The new order should have the created_by field set to the id of its parent order
          child_order['created_by'] = parent_order.id
          
          #o	The new order should have the same pk and platform as its parent order
          child_order['sender_pk'] = parent_order.sender_pk
          child_order['receiver_pk'] = parent_order.receiver_pk
          
          #o	The sell_amount of the new order can be any value such that the implied exchange rate of the new order is at least that of the old order
          #o	You can then try to fill the new order
          #fill_order(child_order)
          
          break

def process_order(order):
    #Your code here
    fill_order(order)
    #
