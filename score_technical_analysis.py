import backtrader as bt
from connect_to_sqlite import get_historical_data_from_db
from connect_to_sqlite import create_table, save_to_sqlite
import pandas as pd
import sqlite3
# Adding scoring to the strategy classes

# 1. MovingAverageCrossover in the short time
class MovingAverageCrossover(bt.Strategy):
    params = (('short_window', 3), ('long_window', 5))

    def __init__(self):
        self.short_mavg = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_window)
        self.long_mavg = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_window)
        self.crossover = bt.indicators.CrossOver(self.short_mavg, self.long_mavg)

    def next(self):
        if self.position.size == 0:
            if self.crossover > 0:
                self.buy()
        elif self.crossover < 0:
            self.close()

    def get_score(self):
        if self.short_mavg[0] > self.long_mavg[0]:
            return 1, "Short-term MA is above Long-term MA."
        else:
            return -1, "Short-term MA is below Long-term MA."

# 2. RsiStrategy
class RsiStrategy(bt.Strategy):
    params = (('rsi_period', 5), ('rsi_lower', 3), ('rsi_upper', 7))

    def __init__(self):
        self.rsi = bt.indicators.RelativeStrengthIndex(self.data.close, period=self.params.rsi_period)

    def next(self):
        if not self.position:
            if self.rsi < self.params.rsi_lower:
                self.buy()
        else:
            if self.rsi > self.params.rsi_upper:
                self.sell()

    def get_score(self):
        if self.rsi[0] < 30:
            return 1, "RSI indicates oversold conditions."
        elif self.rsi[0] > 70:
            return -1, "RSI indicates overbought conditions."
        else:
            return 0, "RSI is neutral."

# 3. BollingerBandsStrategy
class BollingerBandsStrategy(bt.Strategy):
    params = (('period', 1), ('devfactor', 2))

    def __init__(self):
        self.bbands = bt.indicators.BollingerBands(self.data.close, period=self.params.period, devfactor=self.params.devfactor)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.bbands.lines.bot:
                self.buy()
        else:
            if self.data.close[0] < self.bbands.lines.top:
                self.sell()

    def get_score(self):
        if self.data.close[0] > self.bbands.lines.bot[0]:
            return 1, "Stock price is above the lower Bollinger Band."
        elif self.data.close[0] < self.bbands.lines.top[0]:
            return -1, "Stock price is below the upper Bollinger Band."
        else:
            return 0, "Stock price is within the Bollinger Bands."

# 4. BollingerBandsRTM
class BollingerBandsRTM(bt.Strategy):
    params = (("period", 2), ("devfactor", 1.0),)

    def __init__(self):
        self.boll = bt.indicators.BollingerBands(
            self.data.close, period=self.params.period, devfactor=self.params.devfactor
        )

    def next(self):
        if self.data.close[0] < self.boll.lines.bot:
            self.buy()
        elif self.data.close[0] > self.boll.lines.top:
            self.sell()

    def get_score(self):
        if self.data.close[0] < self.boll.lines.bot[0]:
            return 1, "Stock price is below the lower Bollinger Band."
        elif self.data.close[0] > self.boll.lines.top[0]:
            return -1, "Stock price is above the upper Bollinger Band."
        else:
            return 0, "Stock price is within the Bollinger Bands."

# 5. CommodityChannelIndex
class CommodityChannelIndex(bt.Strategy):
    params = (("period", 2), ("threshold", 100),)

    def __init__(self):
        self.cci = bt.indicators.CCI(self.data, period=self.params.period)

    def next(self):
        if self.cci[0] < -self.params.threshold:
            self.buy()
        elif self.cci[0] > self.params.threshold:
            self.sell()

    def get_score(self):
        if self.cci[0] < -100:
            return 1, "CCI indicates a potential price reversal to the upside."
        elif self.cci[0] > 100:
            return -1, "CCI indicates a potential price reversal to the downside."
        else:
            return 0, "CCI is neutral."

# 6. TripleExponentialMovingAverage
class TripleExponentialMovingAverage(bt.Strategy):
    params = (("period", 1),)

    def __init__(self):
        self.trix = bt.indicators.TRIX(self.data, period=self.params.period)

    def next(self):
        if self.trix[0] > self.trix[-1]:
            self.buy()
        elif self.trix[0] < self.trix[-1]:
            self.sell()

    def get_score(self):
        if self.trix[0] > self.trix[-1]:
            return 1, "TRIX is showing upward momentum."
        elif self.trix[0] < self.trix[-1]:
            return -1, "TRIX is showing downward momentum."
        else:
            return 0, "TRIX is neutral."

# 7. RateOfChange (ROC)
class RateOfChange(bt.Strategy):
    params = (("period", 1),)

    def __init__(self):
        self.roc = bt.indicators.RateOfChange(self.data, period=self.params.period)

    def next(self):
        if self.roc[0] > 0:
            self.buy()
        elif self.roc[0] < 0:
            self.sell()

    def get_score(self):
        if self.roc[0] > 0:
            return 1, "Rate of Change indicates positive momentum."
        elif self.roc[0] < 0:
            return -1, "Rate of Change indicates negative momentum."
        else:
            return 0, "Rate of Change is neutral."

# 8. ParabolicSARReversal
class ParabolicSARReversal(bt.Strategy):
    params = (("af", 0.02), ("afmax", 0.2))

    def __init__(self):
        self.sar = bt.indicators.ParabolicSAR(self.data, af=self.params.af, afmax=self.params.afmax)

    def next(self):
        if self.data.close[0] > self.sar[0]:
            self.buy()
        elif self.data.close[0] < self.sar[0]:
            self.sell()

    def get_score(self):
        if self.data.close[0] > self.sar[0]:
            return 1, "Price is above Parabolic SAR indicating bullish trend."
        else:
            return -1, "Price is below Parabolic SAR indicating bearish trend."

# 9. AwesomeOscillatorCross
class AwesomeOscillatorCross(bt.Strategy):
    def __init__(self):
        self.ao = bt.indicators.AwesomeOscillator(self.data)

    def next(self):
        if self.ao[0] > 0 and self.ao[0] > self.ao[-1]:
            self.buy()
        elif self.ao[0] < 0 and self.ao[0] < self.ao[-1]:
            self.sell()

    def get_score(self):
        if self.ao[0] > 0 and self.ao[0] > self.ao[-1]:
            return 1, "Awesome Oscillator is positive and increasing."
        elif self.ao[0] < 0 and self.ao[0] < self.ao[-1]:
            return -1, "Awesome Oscillator is negative and decreasing."
        else:
            return 0, "Awesome Oscillator is neutral."

# 10. HeikinAshiTrend
class HeikinAshiTrend(bt.Strategy):
    def __init__(self):
        self.ha = bt.indicators.HeikinAshi(self.data)

    def next(self):
        if self.ha.ha_close[0] > self.ha.ha_open[0]:
            self.buy()
        elif self.ha.ha_close[0] < self.ha.ha_open[0]:
            self.sell()

    def get_score(self):
        if self.ha.ha_close[0] > self.ha.ha_open[0]:
            return 1, "Heikin Ashi candle is bullish."
        elif self.ha.ha_close[0] < self.ha.ha_open[0]:
            return -1, "Heikin Ashi candle is bearish."
        else:
            return 0, "Heikin Ashi candle is neutral."

# 11. BollingerBandsBreakout
class BollingerBandsBreakout(bt.Strategy):
    params = (('period', 20), ('devfactor', 2),)

    def __init__(self):
        self.bbands = bt.indicators.BollingerBands(self.data.close, period=self.params.period,
                                                   devfactor=self.params.devfactor)

    def next(self):
        if self.data.close[0] > self.bbands.lines.top[0]:
            self.buy()
        elif self.data.close[0] < self.bbands.lines.bot[0]:
            self.sell()

    def get_score(self):
        if self.data.close[0] > self.bbands.lines.top[0]:
            return 1, "Stock price broke above the upper Bollinger Band."
        elif self.data.close[0] < self.bbands.lines.bot[0]:
            return -1, "Stock price broke below the lower Bollinger Band."
        else:
            return 0, "Stock price is within the Bollinger Bands."

# 12. StochasticCross
class StochasticCross(bt.Strategy):
    params = (('period', 14), ('upper', 80), ('lower', 20),)

    def __init__(self):
        self.stoch = bt.indicators.Stochastic(self.data, period=self.params.period)

    def next(self):
        if self.stoch.lines.percK[0] > self.params.upper and self.stoch.lines.percD[0] > self.params.upper:
            self.sell()
        elif self.stoch.lines.percK[0] < self.params.lower and self.stoch.lines.percD[0] < self.params.lower:
            self.buy()

    def get_score(self):
        if self.stoch.lines.percK[0] > self.params.upper and self.stoch.lines.percD[0] > self.params.upper:
            return -1, "Stochastic indicates overbought conditions."
        elif self.stoch.lines.percK[0] < self.params.lower and self.stoch.lines.percD[0] < self.params.lower:
            return 1, "Stochastic indicates oversold conditions."
        else:
            return 0, "Stochastic is neutral."

# 13. TRIXCross
class TRIXCross(bt.Strategy):
    params = (('period', 3),)

    def __init__(self):
        self.trix = bt.indicators.TRIX(self.data.close, period=self.params.period)
        self.signal = bt.indicators.SmoothedMovingAverage(self.trix, period=self.params.period)

    def next(self):
        if self.trix[0] > self.signal[0]:
            self.buy()
        elif self.trix[0] < self.signal[0]:
            self.sell()

    def get_score(self):
        if self.trix[0] > self.signal[0]:
            return 1, "TRIX is above its signal line indicating bullish momentum."
        elif self.trix[0] < self.signal[0]:
            return -1, "TRIX is below its signal line indicating bearish momentum."
        else:
            return 0, "TRIX is neutral with its signal line."

# 14. Momentum
class Momentum(bt.Strategy):
    params = (('period', 2),)

    def __init__(self):
        self.momentum = bt.indicators.Momentum(self.data.close, period=self.params.period)

    def next(self):
        if self.momentum[0] > 0:
            self.buy()
        elif self.momentum[0] < 0:
            self.sell()

    def get_score(self):
        if self.momentum[0] > 0:
            return 1, "Momentum is positive indicating bullish trend."
        elif self.momentum[0] < 0:
            return -1, "Momentum is negative indicating bearish trend."
        else:
            return 0, "Momentum is neutral."

# 15. VolumeBreakout
class VolumeBreakout(bt.Strategy):
    params = (('sma_period', 20), ('volume_multiplier', 2),)

    def __init__(self):
        self.sma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.sma_period)
        self.volume_sma = bt.indicators.SimpleMovingAverage(self.data.volume, period=self.params.sma_period)

    def next(self):
        if self.data.volume[0] > self.volume_sma[0] * self.params.volume_multiplier:
            if not self.position and self.data.close[0] > self.sma[0]:
                self.buy()
            elif self.position and self.data.close[0] < self.sma[0]:
                self.sell()

    def get_score(self):
        if self.data.volume[0] > self.volume_sma[0] * self.params.volume_multiplier:
            if self.data.close[0] > self.sma[0]:
                return 1, "Significant volume breakout detected with price above the moving average."
            else:
                return -1, "Significant volume breakout detected with price below the moving average."
        else:
            return 0, "No significant volume breakout detected."


# 16. VWAPStrategy
class VWAPIndicator(bt.Indicator):
    lines = ('vwap',)
    params = (('period', 5),)

    def __init__(self):
        cum_vol = bt.indicators.CumulativeSum(self.data.volume)
        cum_vol_price = bt.indicators.CumulativeSum(self.data.volume * self.data.close)
        self.lines.vwap = cum_vol_price / cum_vol
class VWAPStrategy(bt.Strategy):
    params = (('vwap_period', 5),)

    def __init__(self):
        self.vwap = VWAPIndicator(self.data, period=self.params.vwap_period)

    def next(self):
        if not self.position:
            if self.data.close[0] > self.vwap[0]:
                self.buy()
        elif self.data.close[0] < self.vwap[0]:
            self.sell()

    def get_score(self):
        if self.data.close[0] > self.vwap[0]:
            return 1, "Price is above VWAP indicating bullish trend."
        elif self.data.close[0] < self.vwap[0]:
            return -1, "Price is below VWAP indicating bearish trend."
        else:
            return 0, "Price is around VWAP indicating a neutral trend."

# 17. Moving Average Convergence Divergence (MACD)
class MACDStrategy(bt.Strategy):
    params = (('fast_length', 12), ('slow_length', 26), ('signal_length', 9),)

    def __init__(self):
        self.macd = bt.indicators.MACD(self.data.close,
                                       period_me1=self.params.fast_length,
                                       period_me2=self.params.slow_length,
                                       period_signal=self.params.signal_length)

    def next(self):
        if self.macd.macd[0] > self.macd.signal[0] and self.macd.macd[-1] <= self.macd.signal[-1]:
            self.buy()
        elif self.macd.macd[0] < self.macd.signal[0] and self.macd.macd[-1] >= self.macd.signal[-1]:
            self.sell()

    def get_score(self):
        if self.macd.macd[0] > self.macd.signal[0]:
            return 1, "MACD line is above the signal line indicating bullish momentum."
        elif self.macd.macd[0] < self.macd.signal[0]:
            return -1, "MACD line is below the signal line indicating bearish momentum."
        else:
            return 0, "MACD line is crossing the signal line."

# 18. On-Balance Volume (OBV)
class OnBalanceVolume(bt.Indicator):
    lines = ('obv',)
    alias = ('OBV',)

    def __init__(self):
        self.addminperiod(2)  # Ensure there's at least two data points
        self.lines.obv = bt.indicators.CumulativeSum(self.get_volume_diff())

    def get_volume_diff(self):
        return bt.If(self.data.close(0) > self.data.close(-1), self.data.volume, -self.data.volume)

class OBVStrategy(bt.Strategy):

    def __init__(self):
        self.obv = OnBalanceVolume(self.data)

    def next(self):
        if not self.position:
            if self.obv[0] > self.obv[-1]:
                self.buy()
        else:
            if self.obv[0] < self.obv[-1]:
                self.sell()

    def get_score(self):
        if self.obv[0] > self.obv[-1]:
            return 1, "On-Balance Volume is increasing, indicating buying pressure."
        elif self.obv[0] < self.obv[-1]:
            return -1, "On-Balance Volume is decreasing, indicating selling pressure."
        else:
            return 0, "On-Balance Volume is stable."


# 19. Moving Averages in the long time
class SMAStrategy(bt.Strategy):
    params = (('short_window', 50), ('long_window', 200),)

    def __init__(self):
        self.short_sma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.short_window)
        self.long_sma = bt.indicators.SimpleMovingAverage(self.data.close, period=self.params.long_window)

    def next(self):
        if self.short_sma[0] > self.long_sma[0] and self.short_sma[-1] <= self.long_sma[-1]:
            self.buy()
        elif self.short_sma[0] < self.long_sma[0] and self.short_sma[-1] >= self.long_sma[-1]:
            self.sell()

    def get_score(self):
        if self.short_sma[0] > self.long_sma[0]:
            return 1, "Short-term MA is above Long-term MA in the long time."
        else:
            return -1, "Short-term MA is below Long-term MA in the long time."

# 20. Support and Resistance Levels
class SupportResistanceStrategy(bt.Strategy):
    params = (('support_resistance_period', 50),)

    def __init__(self):
        self.support = bt.indicators.Lowest(self.data.low, period=self.params.support_resistance_period)
        self.resistance = bt.indicators.Highest(self.data.high, period=self.params.support_resistance_period)

    def next(self):
        if self.data.close[0] > self.resistance[0]:
            self.buy()
        elif self.data.close[0] < self.support[0]:
            self.sell()

    def get_score(self):
        if self.data.close[0] > self.resistance[0]:
            return 1, "Price broke above resistance."
        elif self.data.close[0] < self.support[0]:
            return -1, "Price broke below support."
        else:
            return 0, "Price is within support and resistance."


# Function to run the backtest with a given strategy
def run_backtest(strategy_class, symbol, historical_data):
    cerebro = bt.Cerebro()
    cerebro.addstrategy(strategy_class)
    data = bt.feeds.PandasData(dataname=historical_data[historical_data['Symbol'] == symbol])
    cerebro.adddata(data)
    strats = cerebro.run()
    return strats[0].get_score()  # Note: This now returns a tuple (score, analysis_message)


def calculate_total_score():
    Stragegies = [
        MovingAverageCrossover, BollingerBandsStrategy, RsiStrategy,
        BollingerBandsRTM, CommodityChannelIndex, TripleExponentialMovingAverage,
        RateOfChange, ParabolicSARReversal, AwesomeOscillatorCross, HeikinAshiTrend,
        BollingerBandsBreakout, StochasticCross, TRIXCross, Momentum, VolumeBreakout,
        VWAPStrategy, MACDStrategy, OBVStrategy, SMAStrategy, SupportResistanceStrategy
    ]
    create_table()
    start_date_score = '2022-01-01'
    end_date_score = pd.Timestamp.now()
    conn = sqlite3.connect('db.sqlite3')
    cursor = conn.cursor()
    cursor.execute(f"SELECT MAX(Timestamp) FROM technical_analysis_score")
    max_date = cursor.fetchone()[0]
    if max_date is not None:
        current_date = pd.Timestamp(max_date) + pd.DateOffset(1)
    else:
        current_date = pd.Timestamp(start_date_score)

    while current_date <= end_date_score:
        date_str = current_date.strftime('%Y-%m-%d')
        historical_data = get_historical_data_from_db(date_str)

        current_date += pd.Timedelta(days=1)

        symbols_list = historical_data['Symbol'].unique()
        results = []

        for Symbol in symbols_list:
            total_score = 0
            strategy_analysis = []  # List to accumulate the analysis messages
            print("Technical Score for " + Symbol + " on " + date_str + "...")

            end_date = date_str

            for strategy in Stragegies:
                try:
                    score, analysis_message = run_backtest(strategy, Symbol, historical_data)
                    total_score += score
                    strategy_analysis.append(analysis_message)  # Add the analysis message to the list
                except Exception as e:
                    print(f"Error calculating score using {strategy} for {Symbol}: {str(e)}")

            # Combine the accumulated analysis messages into a single string
            combined_analysis = '\n'.join(strategy_analysis)

            # Determine the overall sentiment based on total score
            sentiment_analysis = f"Overall {Symbol} is Neutral for our 20 technical analysis.\n"
            if total_score > 0:
                sentiment_analysis = f"Overall {Symbol} is Bearish for our 20 technical analysis.\n"
            elif total_score < 0:
                sentiment_analysis = f"Overall {Symbol} is Bullish for our 20 technical analysis.\n"

            # Combine the sentiment analysis with the combined analysis from the strategies
            final_analysis = f"{sentiment_analysis}Details: \n{combined_analysis}"

            print(f"Total score for {Symbol}: {total_score}. Analysis: {final_analysis}\n")
            results.append((Symbol, total_score, final_analysis))

        df_results = pd.DataFrame(results, columns=['Symbol', 'Score', 'Analysis'])
        df_results['Rank'] = df_results['Score'].rank(ascending=False, method='min').astype(int)


        for index, row in df_results.iterrows():
            save_to_sqlite(row['Symbol'], row['Score'], row['Rank'], row['Analysis'], end_date)

        print(df_results)

# calculate_total_score()