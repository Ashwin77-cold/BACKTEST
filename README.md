import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re

# -------------------------------
# Helper Functions
# -------------------------------

def load_spot_data(date_str):
    """
    Load the SPOT CSV file for the given date_str (format: DDMMYYYY).
    Files are assumed to be at: X:/Ashwin/backtest/SPOT/SENSEX_<date_str>.csv
    """
    spot_file = f"X:/Ashwin/backtest/SPOT/SENSEX_{date_str}.csv"
    if os.path.exists(spot_file):
        return pd.read_csv(spot_file)
    else:
        print(f"Spot file not found for {date_str}")
        return None

def load_options_data(date_str):
    """
    Load the OPTIONS CSV file for the given date_str (format: DDMMYYYY).
    Files are assumed to be at: X:/Ashwin/backtest/OPTIONS/SENSEX_{date_str}.csv
    """
    options_file = f"X:/Ashwin/backtest/OPTIONS/SENSEX_{date_str}.csv"
    if os.path.exists(options_file):
        return pd.read_csv(options_file)
    else:
        print(f"Options file not found for {date_str}")
        return None

def extract_strike(ticker):
    """
    Extracts the strike from a ticker.
    Expected ticker format: SENSEXDDMMMYY<strike><OptionType>
    For example: "SENSEX05JAN2468500PE" should yield strike = 68500.
    (This regex assumes the strike is a 5-digit number.)
    """
    match = re.search(r'SENSEX\d{2}[A-Za-z]{3}\d{2}(\d{5})(PE|CE)', ticker)
    if match:
        return int(match.group(1))
    return None

def get_option_price(options_df, option_type, strike, target_time):
    """
    Returns the option's "Close" price for a given option_type and strike at target_time.
    If an exact match on Time is not found, returns the earliest available price after target_time.
    Assumes options_df has a 'Time' column in HH:MM:SS format.
    """
    df = options_df[(options_df['Ticker'].str.contains(option_type)) & (options_df['Strike'] == strike)]
    row = df[df['Time'] == target_time]
    if not row.empty:
        return float(row['Close'].iloc[0])
    # Else, get the earliest available after target_time:
    df = df.copy()
    df['Time_dt'] = pd.to_datetime(df['Time'], format='%H:%M:%S')
    target_dt = datetime.strptime(target_time, '%H:%M:%S')
    df_after = df[df['Time_dt'] >= target_dt]
    if not df_after.empty:
        return float(df_after.iloc[0]['Close'])
    return None

def get_sorted_times(options_df, option_type, strike, start_time):
    """
    Returns a sorted list of available Time strings (HH:MM:SS) for the given option type and strike,
    starting from start_time.
    """
    df = options_df[(options_df['Ticker'].str.contains(option_type)) & (options_df['Strike'] == strike)]
    times = df['Time'].unique()
    times = sorted(times, key=lambda t: datetime.strptime(t, '%H:%M:%S'))
    target_dt = datetime.strptime(start_time, '%H:%M:%S')
    return [t for t in times if datetime.strptime(t, '%H:%M:%S') >= target_dt]

# -------------------------------
# Simulation of a Single Day
# -------------------------------

def simulate_day(date_str):
    """
    Simulates one trading day for the given date_str (format: DDMMYYYY) with the following steps:
      1. At ~09:16:59, get the SPOT price, round to nearest 100.
         Set CE strike = rounded_spot + 100 and PE strike = rounded_spot - 100.
      2. Sell both CE and PE at their 09:16:59 prices.
      3. Apply a combined SL: if the sum of current option prices (CE + PE) reaches 220, an SL event is triggered.
      4. At that moment, determine which leg has the greater adverse move.
         (For example, if CE goes from 100 to 90 and PE goes from 100 to 130, then combined = 90 + 130 = 220.
          Since PE lost 30 (i.e. moved up from 100 to 130) and CE gained 10 (moved down from 100 to 90),
          PE is the losing leg.)
      5. Cover (buy) the losing leg at that time and adjust the SL of the remaining leg to its original sell price.
      6. At the next available time, re-enter on the losing leg **by placing a BUY order** (i.e. re-enter on the buying side).
         For the re-entry order, set a new SL of 15% below the re-entry price and a target of:
             re-entry price + (10% of the original sell price + loss points from the initial strangle).
    Returns a list (order book) of all trade events for the day.
    """
    order_book = []
    
    # Load SPOT and OPTIONS data:
    spot_data = load_spot_data(date_str)
    options_data = load_options_data(date_str)
    if spot_data is None or options_data is None:
        return None

    # Ensure options data has Strike extracted:
    options_data['Strike'] = options_data['Ticker'].apply(extract_strike)

    # --- 1. Initial Entry at ~09:16:59 ---
    row_spot = spot_data[spot_data['Time'] == '09:16:59']
    if row_spot.empty:
        spot_data['Time_dt'] = pd.to_datetime(spot_data['Time'], format='%H:%M:%S')
        target_dt = datetime.strptime('09:16:59', '%H:%M:%S')
        row_spot = spot_data[spot_data['Time_dt'] >= target_dt]
        if row_spot.empty:
            print(f"No SPOT data available after 09:16:59 for {date_str}")
            return None
        spot_price = float(row_spot.iloc[0]['Close'])
    else:
        spot_price = float(row_spot.iloc[0]['Close'])
    print(f"Date {date_str} - Spot Price at 09:16:59: {spot_price}")
    
    # Round the SPOT to the nearest 100 and determine strikes:
    rounded_spot = round(spot_price / 100) * 100
    ce_strike = rounded_spot + 100
    pe_strike = rounded_spot - 100
    print(f"Rounded Spot: {rounded_spot} -> CE Strike: {ce_strike}, PE Strike: {pe_strike}")

    # Get the sell prices at 09:16:59:
    ce_sell_price = get_option_price(options_data, 'CE', ce_strike, '09:16:59')
    pe_sell_price = get_option_price(options_data, 'PE', pe_strike, '09:16:59')
    if ce_sell_price is None or pe_sell_price is None:
        print(f"Could not obtain sell prices for one or both legs on {date_str}")
        return None

    # Record initial short orders:
    order_book.append({
        'Date': date_str,
        'Time': '09:16:59',
        'Action': 'Sell',
        'Option': 'CE',
        'Strike': ce_strike,
        'Price': ce_sell_price,
        'Note': 'Initial Sell'
    })
    order_book.append({
        'Date': date_str,
        'Time': '09:16:59',
        'Action': 'Sell',
        'Option': 'PE',
        'Strike': pe_strike,
        'Price': pe_sell_price,
        'Note': 'Initial Sell'
    })

    # --- 2. Monitor for SL Event ---
   
    combined_SL_threshold = (ce_sell_price + pe_sell_price)*1.10

    # Get available monitoring times from options data starting at 09:17:00:
    monitor_times = get_sorted_times(options_data, 'CE', ce_strike, '09:17:00')
    
    # Flags and initial prices for loss calculation:
    ce_active = True
    pe_active = True
    initial_ce_sell = ce_sell_price  # e.g., 100
    initial_pe_sell = pe_sell_price  # e.g., 100
    sl_event_handled = False
    sl_trigger_time = None
    loss_points = None

    for t in monitor_times:
        current_ce_price = get_option_price(options_data, 'CE', ce_strike, t)
        current_pe_price = get_option_price(options_data, 'PE', pe_strike, t)
        if current_ce_price is None or current_pe_price is None:
            continue

        # Calculate combined current premium:
        combined_price = current_ce_price + current_pe_price

        if not sl_event_handled and combined_price >= combined_SL_threshold:
            sl_event_handled = True
            sl_trigger_time = t
            # Calculate adverse move (loss) for each leg:
            ce_loss = current_ce_price - initial_ce_sell   # For CE, a higher price is a loss for a short
            pe_loss = current_pe_price - initial_pe_sell       # For PE, a higher price is a loss
            # Determine which leg lost more:
            if pe_loss > -ce_loss:  # e.g., if CE went to 90 (loss = -10) and PE went to 130 (loss = +30)
                hit_leg = 'PE'
                loss_points = current_pe_price - initial_pe_sell  # e.g., 130 - 100 = 30
                order_book.append({
                    'Date': date_str,
                    'Time': t,
                    'Action': 'Buy',
                    'Option': 'PE',
                    'Strike': pe_strike,
                    'Price': current_pe_price,
                    'Note': f'SL Hit on PE at loss {pe_loss:.1f} points, covering'
                })
                pe_active = False
                # Adjust SL of remaining leg (CE) to its original sell price:
                order_book.append({
                    'Date': date_str,
                    'Time': t,
                    'Action': 'Adjust SL',
                    'Option': 'CE',
                    'Strike': ce_strike,
                    'New SL': initial_ce_sell,
                    'Note': 'Shift SL to original sell price for CE'
                })
                losing_leg = 'PE'
            else:
                hit_leg = 'CE'
                loss_points = current_ce_price - initial_ce_sell  # e.g., if CE moved adversely more
                order_book.append({
                    'Date': date_str,
                    'Time': t,
                    'Action': 'Buy',
                    'Option': 'CE',
                    'Strike': ce_strike,
                    'Price': current_ce_price,
                    'Note': f'SL Hit on CE at loss {loss_points:.1f} points, covering'
                })
                ce_active = False
                order_book.append({
                    'Date': date_str,
                    'Time': t,
                    'Action': 'Adjust SL',
                    'Option': 'PE',
                    'Strike': pe_strike,
                    'New SL': initial_pe_sell,
                    'Note': 'Shift SL to original sell price for PE'
                })
                losing_leg = 'CE'
            break  # Process only one SL event per day

    # --- 3. Re-entry on the Losing Leg (BUY order) ---
    if sl_event_handled:
        # Find next available time after the SL trigger for re-entry:
        reentry_time = None
        for t_candidate in monitor_times:
            if datetime.strptime(t_candidate, '%H:%M:%S') > datetime.strptime(sl_trigger_time, '%H:%M:%S'):
                reentry_time = t_candidate
                break
        if reentry_time is None:
            reentry_time = monitor_times[-1]
        
        if losing_leg == 'PE':
            reentry_price = get_option_price(options_data, 'PE', pe_strike, reentry_time)
            # For a BUY re-entry, new stop loss is 15% below the re-entry price:
            new_sl = reentry_price * 0.85
            # For a BUY order, the target is re-entry price + (10% of original sell + loss_points)
            target_price = reentry_price + (0.10 * initial_pe_sell + loss_points)
            order_book.append({
                'Date': date_str,
                'Time': reentry_time,
                'Action': 'Re-entry Buy',
                'Option': 'PE',
                'Strike': pe_strike,
                'Price': reentry_price,
                'New SL': new_sl,
                'Target': target_price,
                'Note': f'Re-entry on PE (BUY): SL=15% below re-entry price, Target = re-entry price + (10% + {loss_points:.2f})'
            })
        elif losing_leg == 'CE':
            reentry_price = get_option_price(options_data, 'CE', ce_strike, reentry_time)
            new_sl = reentry_price * 0.85
            target_price = reentry_price + (0.10 * initial_ce_sell + loss_points)
            order_book.append({
                'Date': date_str,
                'Time': reentry_time,
                'Action': 'Re-entry Buy',
                'Option': 'CE',
                'Strike': ce_strike,
                'Price': reentry_price,
                'New SL': new_sl,
                'Target': target_price,
                'Note': f'Re-entry on CE (BUY): SL=15% below re-entry price, Target = re-entry price + (10% + {loss_points:.2f})'
            })
    else:
        order_book.append({
            'Date': date_str,
            'Time': 'EndOfDay',
            'Action': 'Hold',
            'Note': 'No SL or target hit during the day'
        })
    
    return order_book

# -------------------------------
# Backtest Over Multiple Dates
# -------------------------------

def backtest_all(dates):
    all_orders = []
    for d in dates:
        print(f"Processing date: {d}")
        day_orders = simulate_day(d)
        if day_orders:
            all_orders.extend(day_orders)
    return all_orders

# -------------------------------
# Generate Date List from 2023-01-01 to 2024-12-31
# -------------------------------

start_date = datetime(2023, 1, 1)
end_date = datetime(2024, 12, 31)
date_list = []
current_date = start_date
while current_date <= end_date:
    date_list.append(current_date.strftime("%d%m%Y"))
    current_date += timedelta(days=1)

# -------------------------------
# Run Backtest and Save Order Book
# -------------------------------

orders = backtest_all(date_list)
orderbook_df = pd.DataFrame(orders)
output_file = "D:/Ashwin/backtest_orderbook_all_dates.csv"
orderbook_df.to_csv(output_file, index=False)
print(f"Backtest completed. Order book saved to {output_file}")
