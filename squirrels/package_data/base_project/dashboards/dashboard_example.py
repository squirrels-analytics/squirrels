from squirrels import DashboardArgs, Dashboard
from matplotlib import pyplot as plt, figure as f, axes as a


def main(sqrl: DashboardArgs) -> Dashboard:
    """
    Create a dashboard by joining/processing dependent datasets and/or other dashboards to
    form and return the result as a new pandas DataFrame.
    """
    fig: f.Figure; ax0: a.Axes; ax1: a.Axes
    fig, (ax0, ax1) = plt.subplots(1, 2)

    # Create a bar chart of spending by month
    spending_by_month_df = sqrl.dataset("dataset_example", group_by="g4")
    spending_by_month_df.plot(x="month", y="amount", ax=ax0)
    ax0.set_title("Spending by Month")

    # Create a pie chart of spending by subcategory
    spending_by_subcategory_df = sqrl.dataset("dataset_example", group_by="g3")
    spending_by_subcategory_df.plot(x="subcategory", y="amount", ax=ax1)
    ax1.set_title("Spending by Subcategory")
    
    return Dashboard(fig)
