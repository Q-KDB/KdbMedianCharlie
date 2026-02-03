/ ==============================================================================
/ SECTION 1: Environment & Dynamic Path Loading
/ ==============================================================================
show "--- Initializing Unified Pipeline ---";

dbPathStr: "C:/Users/charl/OneDrive/桌面/q/kdb/db";
dbPath: hsym `$dbPathStr;

/ Initial Load and Directory Check
if[()~key dbPath; .[system;("mkdir \"",ssr[dbPathStr;"/";"\\"],"\"");{}]];
system "l ", dbPathStr;

/ Parameters
startDate: 2023.01.02;
endDate: 2025.01.02;
allDays: startDate + til 1 + endDate - startDate;
tradingDays: allDays where (mod[allDays; 7]) < 5; 
marketMinutes: 390;
barTimes: 09:30:00 + 00:01 * til marketMinutes; 
syms: `IBM`AAPL`TSLA; 
basePrices: 185.0 185.0 250.0; 

/ ==============================================================================
/ SECTION 2: Step 1 - Simulation & In-Memory Storage
/ ==============================================================================
show "Step 1: Running Price Simulation...";

simulate_day_one_sym: {[dt; sym; startP]
    n: marketMinutes;
    returns: 0.001 * sums (n?1.0) - 0.5; 
    prices: startP * 1 + returns;
    ([] date: dt; bar_time: barTimes; sym: n#sym; price: prices)
 };

construct_table_one_day: {[dt; openP]
    t: raze { [dt; sym; op] simulate_day_one_sym[dt; sym; op] }[dt] ' [syms; openP]; 
    `date`bar_time xasc t
 };

openP: basePrices; 
all_prices: (); / We build this in memory to avoid repeated disk reads

i: 0;
while[i < count tradingDays;
    dt: tradingDays[i];
    day_table: construct_table_one_day[dt; openP];
    
    / Persist to disk
    prices_table_part: select bar_time, sym, price from day_table; 
    .Q.dpft[dbPath; dt; `sym; `prices_table_part]; 
    
    / Add to master memory table
    all_prices,: day_table;
    
    openP: exec last price by sym from day_table; 
    if[0 = i mod 50; show "Partition saved: ", string dt];
    i+: 1
 ];

/ ==============================================================================
/ SECTION 3: Step 2 & 3 - Returns and Moving Medians
/ ==============================================================================
show "Step 2: Calculating Returns & Analytics...";

/ Returns
analytics: update return: (price - prev price) % prev price by date, sym from all_prices;

/ Step 3: Moving Medians (Window 7)
analytics: update moving_med: mavg[7; price] by sym, bar_time from analytics;

/ Step 4: Exponential Nudge Median (Alpha 0.5 for better correlation)
alpha: 0.5;
exp_step: {[a; m; p] $[p > m; m + a; p < m; m - a; m]};
analytics: update exp_med: exp_step[alpha;;]\[first price; price] by date, sym from analytics;

/ Save full analytics to disk
{ [dt; data]
    final_tab:: select bar_time, sym, price, exp_med from data where date = dt;
    .Q.dpft[dbPath; dt; `sym; `final_tab]
 }[;analytics] each tradingDays;

/ ==============================================================================
/ SECTION 4: Step 5 - Correlation & Regression (The Math)
/ ==============================================================================
show "Step 4: Running Statistical Analysis...";

/ Filter for one symbol to ensure identical counts
sym_filter: `AAPL;
reg_tab: select price:1f*price, exp_med:1f*exp_med from analytics where sym=sym_filter;

/ A. Correlation
p: exec price from reg_tab;
m: exec exp_med from reg_tab;
show "AAPL Correlation: ", string p cor m;

/ B. Simple Linear Regression (Price ~ Median)
/ X (Intercept and Median) on the LEFT
y: p;
x_simple: ((count y)#1f; m);
simple_coeffs: x_simple lsq y;
show "Simple Coeffs (Intercept; Slope):"; show simple_coeffs;

/ C. Multi-Linear Regression (Price ~ Median + Time Index)
x_multi: ((count y)#1f; m; 1f*til count y);
multi_coeffs: x_multi lsq y;
show "Multi Coeffs (Intercept; Median_Slope; Time_Slope):"; show multi_coeffs;

/ ==============================================================================
/ SECTION 5: Final Report
/ ==============================================================================
show "Step 5: Generating Symbol Report...";

final_report: select 
    full_corr: price cor exp_med,
    last_21_corr: (-21#price) cor (-21#exp_med) 
    by sym from analytics where date = last tradingDays;

show final_report;
show "--- Pipeline Completed Successfully ---";