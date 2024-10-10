import pandas as pd
import os


def load_data(file_path):
    return pd.read_csv(file_path)


def process_trades(trades_df, purchase_price_df):

    trades_df['purchase_price'] = purchase_price_df['Amount'] # merge

    trades_df['Interest Rate Exp'] = trades_df['Interest rate exp'].str.rstrip('%').astype('float') / 100.0

    trades_df['Status'] = trades_df['Status'].apply(lambda x: 'Closed' if x.lower() == 'closed' else 'Outstanding')

    trades_df = trades_df.rename(columns={
        'Trade ID': 'trade_id',
        'Issue date': 'issue_date',
        'Currency': 'currency',
        'Due date': 'due_date',
        'Interest Rate Exp': 'interest_rate_exp',
        'Status': 'status',
        'Purchase Price': 'purchase_price'
    })

    return trades_df[
        ['trade_id', 'issue_date', 'currency', 'due_date', 'interest_rate_exp', 'status', 'purchase_price']]


def process_cash_flows(cash_flows_df, trades_df, purchase_price_df):

    interest_rate = trades_df['Interest rate exp'].str.rstrip('%').astype('float') / 100.0
    cash_flows_df['Amount'] = abs(cash_flows_df['Amount']) * (1 - interest_rate)

    cash_flows_df = cash_flows_df.rename(columns={
        'Trade ID': 'trade_id',
        'Cash flow date': 'cash_flow_date',
        'Cash flow currency': 'cash_flow_currency',
        'Cash flow type': 'cash_flow_type',
        'Amount': 'amount'
    })

    print(cash_flows_df)

    return cash_flows_df[['trade_id', 'cash_flow_date', 'cash_flow_currency', 'amount', 'cash_flow_type']]


def main():
    input_directory = "data/input/"
    output_directory = "data/output/"

    trades_df = load_data(os.path.join(input_directory, "trades.csv"))
    cash_flows_df = load_data(os.path.join(input_directory, "cash_flows.csv"))
    purchase_price_df = load_data(os.path.join(input_directory, "purchase_price.csv"))

    processed_trades_df = process_trades(trades_df, purchase_price_df)
    processed_cash_flows_df = process_cash_flows(cash_flows_df, trades_df, purchase_price_df)

    processed_trades_df.to_csv(os.path.join(output_directory, "trades.csv"), index=False)
    processed_cash_flows_df.to_csv(os.path.join(output_directory, "cash_flows.csv"), index=False)


if __name__ == "__main__":
    main()