'use strict';

const elem = React.createElement;

function refreshWidgets(target = null) {

}

function processState(stateMap, name, selection) {
    const [selected, setSelected] = React.useState(selection);
    stateMap.set(name, selected);

    return (targetValue, trigger_refresh) => {
        setSelected(targetValue);
        if (trigger_refresh) {
            refreshWidgets({ [name]: targetValue });
        }
    }
}

function createWidgetDiv(label, widgetElement) {
    return (
        <div className="widget-div">
            <label className="widget-label" htmlFor={widgetElement.id}>{label}</label>
            {widgetElement}
        </div>
    );
}

function createOptionsDom(options) {
    return options.map(option =>
        <option key={option.id} value={option.id}>{option.label}</option>
    );
}

function SingleSelectWidget({ name, label, options, trigger_refresh, selected_id, stateMap }) {
    if (options.length === 0) return null;

    const handleChange = processState(stateMap, name, selected_id);
    const optionsDom = createOptionsDom(options);
    const widgetDom = (
        <select
            id={name} 
            className="single-select widget"
            defaultValue={selected_id}
            onChange={e => handleChange(e.target.value, trigger_refresh)}
        >
            {optionsDom}
        </select>
    );
    return createWidgetDiv(label, widgetDom);
}

function MultiSelectWidget({ name, label, options, include_all, order_matters, trigger_refresh, selected_ids, stateMap }) {
    if (options.length === 0) return null;
    
    const handleChange = processState(stateMap, name, selected_ids);
    const optionsDom = createOptionsDom(options);
    const getSelectedOptions = (target => [...target.selectedOptions].map(option => option.value));
    const widgetDom = (
        <select multiple
            id={name} 
            className="multi-select widget" 
            defaultValue={selected_ids}
            onChange={e => handleChange(getSelectedOptions(e.target), trigger_refresh)}
        >
            {optionsDom}
        </select>
    );
    return createWidgetDiv(label, widgetDom);
}

function DateWidget({ name, label, selected_date, stateMap }) {
    const handleChange = processState(stateMap, name, selected_date);
    const widgetDom = <input type="date" 
        id={name} 
        defaultValue={selected_date} 
        onChange={e => handleChange(e.target.value, false)} 
    />
    return createWidgetDiv(label, widgetDom)
}

const optionsSingle = [
    {
        "id": "g0",
        "label": "Transaction"
    },
    {
        "id": "g1",
        "label": "Date"
    },
    {
        "id": "g2",
        "label": "Category"
    },
    {
        "id": "g3",
        "label": "Subcategory"
    }
]

const optionsMulti = [
    {
        "id": "0",
        "label": "Food"
    },
    {
        "id": "1",
        "label": "Bills"
    }
]

const stateMap = new Map();
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
    <SingleSelectWidget 
        name="group_by" label="Group By" 
        options={optionsSingle} 
        trigger_refresh={false} 
        selected_id="g2" 
        stateMap={stateMap}
    />
    // <MultiSelectWidget
    //     name="category" label="Category Filter"
    //     include_all={true}
    //     order_matters={false}
    //     options={optionsMulti}
    //     trigger_refresh={false} 
    //     selected_ids={["0", "1"]}
    //     stateMap={stateMap}
    // />
    // <DateWidget name="start_date" label="Start Date" selected_date="2023-01-01" stateMap={stateMap} />
);
