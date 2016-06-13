<DEALS>{
for $amount in fn:doc("property.xml")//RESIDENTIAL//RENT_AMOUNT[. < 800]
return (<DEAL>{$amount}</DEAL>)
}</DEALS>
