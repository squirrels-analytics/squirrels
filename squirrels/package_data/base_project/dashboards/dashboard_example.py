from squirrels import DashboardArgs, dashboards as d
from matplotlib import pyplot as plt, figure as f, axes as a


async def main(sqrl: DashboardArgs) -> d.PngDashboard:
    spending_by_month_df = await sqrl.dataset("federate_dataset_example", fixed_parameters={"group_by": "g4"})
    spending_by_subcategory_df = await sqrl.dataset("federate_dataset_example", fixed_parameters={"group_by": "g3"})

    # Create a figure with two subplots
    fig, (ax0, ax1) = plt.subplots(2, 1, figsize=(8, 8), height_ratios=(1, 2))
    fig: f.Figure; ax0: a.Axes; ax1: a.Axes
    fig.tight_layout(pad=4, h_pad=6)

    # Create a bar chart of spending by month

    # Convert to pandas and ensure total_amount is numeric
    spending_by_month_pandas = spending_by_month_df.sort("month").to_pandas()
    spending_by_month_pandas["total_amount"] = spending_by_month_pandas["total_amount"].astype(float)

    spending_by_month_pandas.plot(x="month", y="total_amount", ax=ax0)
    ax0.set_title("Spending by Month")

    # Create a pie chart of spending by subcategory

    # Convert to pandas and ensure total_amount is numeric
    subcategory_pandas = spending_by_subcategory_df.sort("total_amount", descending=True).to_pandas()
    subcategory_pandas["total_amount"] = subcategory_pandas["total_amount"].astype(float)
    subcategory_pandas.set_index("subcategory", inplace=True)
    
    autopct = lambda pct: ('%.1f%%' % pct) if pct > 6 else ''
    subcategory_pandas.plot(y="total_amount", kind='pie', ax=ax1, autopct=autopct, legend=False, ylabel="")
    ax1.set_title("Spending by Subcategory")
    
    return d.PngDashboard(fig)
