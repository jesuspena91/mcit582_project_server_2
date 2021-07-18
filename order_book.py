from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

from models import Base, Order
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()

def process_order(order):
    #Your code here
    
    # Check if there are any existing orders that match
    query = (session.query(Order)
              .filter(Order.filled == None)
              .filter(Order.buy_currency == order['sell_currency'])
              .filter(Order.sell_currency == order['buy_currency'])
              .filter((Order.sell_amount/Order.buy_amount) >= (order['buy_amount']/order['sell_amount']))
              )
    
    # Inserting order in database
    new_order = Order( sender_pk=order['sender_pk'],
                      receiver_pk=order['receiver_pk'], 
                      buy_currency=order['buy_currency'], 
                      sell_currency=order['sell_currency'], 
                      buy_amount=order['buy_amount'], 
                      sell_amount=order['sell_amount'] )
    session.add(new_order)
    session.commit()
    
    if query.count() > 0:
      existing_order = query.first()
      
      # Set the filled field to be the current timestamp on both orders
      new_order.filled = datetime.now()
      existing_order.filled = datetime.now()
      session.commit()
      
      # Set counterparty_id to be the id of the other order
      new_order.counterparty_id = existing_order.id
      existing_order.counterparty_id = new_order.id
      session.commit()
      
      # If one of the orders is not completely filled 
      # (i.e. the counterparty’s sell_amount is less than buy_amount)
      if new_order.buy_amount < existing_order.sell_amount:
        remaining_buy = existing_order.sell_amount - new_order.buy_amount
        remaining_sell = existing_order.buy_amount - new_order.sell_amount
        
        if (remaining_buy > 0  and remaining_sell > 0 and ()):
          derived_order = Order( sender_pk=existing_order.sender_pk,
                        receiver_pk=existing_order.receiver_pk, 
                        buy_currency=existing_order.buy_currency, 
                        sell_currency=existing_order.sell_currency, 
                        buy_amount=remaining_sell, 
                        sell_amount=remaining_buy,
                        creator_id=existing_order.id)
          session.add(derived_order)
          session.commit()
      
      elif new_order.buy_amount > existing_order.sell_amount:
        remaining_buy = new_order.buy_amount - existing_order.sell_amount
        remaining_sell = new_order.sell_amount - existing_order.buy_amount
        
        if (remaining_buy > 0  and remaining_sell > 0):
          derived_order = Order( sender_pk=new_order.sender_pk,
                        receiver_pk=new_order.receiver_pk, 
                        buy_currency=new_order.buy_currency, 
                        sell_currency=new_order.sell_currency, 
                        buy_amount=remaining_buy, 
                        sell_amount=remaining_sell,
                        creator_id=new_order.id)
          session.add(derived_order)
          session.commit()
    
    pass