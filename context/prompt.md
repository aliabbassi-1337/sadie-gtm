and now we can test every bit to not get the same bugs we were getting with cloudbeds so

anws did you look into how we could potentially scrape the email from the property description?

take this page for example https://bookings12.rmscloud.com/search/index/2633/3 

look at all this data we could get 

Property Logo 
Currawong Beach Cottages
Welcome to Currawong Beach Cottages!

Minimum Stay Requirements:
4 nights during December, January, Easter, and school holidays
3 nights on public holiday weekends
2 nights at all other times
Advance Bookings:
Bookings can be made up to 12 months in advance.

For group bookings, please email beachcottages@currawong.com.au. Day visitor numbers are restricted and permission must be granted by Management.

Arrival Date 
Departure Date 
Number of Guests 
2 Gu
Promotional/Group Code
Search Availability
Access Guest PortalTerms & ConditionsCancellation PolicyTravel DirectionsFeaturesThings To DoBusiness FeaturesCar ParkingPet Policy
Currawong Beach Cottages  0403 606 667  beachcottages@currawong.com.au
Powered by RMS 5.25.345.4


html structure

<div style="clear:both;">
                    <!--id="content-marker" class="content"-->
                    <div style="text-align:center; padding-top:10px;">

                            <div class="logo-search global-logo">
                                        <img src="https://images.rmscloud.com/rmsoimages/2633/RMSWin/RMSOnlineImages/00000235.png" alt="Property Logo">

                            </div>
                                <div class="prop-name-search" style="color:#000;">
                                    <h1>Currawong Beach Cottages</h1>
                                </div>



                    </div>
                    



<div class="clearfix"></div>

    <div class="box-800" style="margin-bottom:20px; max-height:340px; overflow-y:auto;">
        <p class="portfolio-description" style="color:#000;">
            Welcome to Currawong Beach Cottages!  <br><br>Minimum Stay Requirements:<br>4 nights during December, January, Easter, and school holidays<br>3 nights on public holiday weekends<br>2 nights at all other times<br>Advance Bookings:<br>Bookings can be made up to 12 months in advance.<br><br>For group bookings, please email beachcottages@currawong.com.au.  Day visitor numbers are restricted and permission must be granted by  Management.
        </p>
    </div>



<div class="box-800">
        <div id="start-date-div" class="one-half-responsive clearfix">
            <label for="arriveDate">Arrival Date <span class="asterisk_input"></span></label>
            <div class="div-cal-icon">
                <input class="icon-field" id="arriveDate" name="A" readonly="" type="text" value="" sd_sf="ovmm">
            </div>
        </div>
        <div class="one-half-responsive last">
            <label for="departDate">Departure Date <span class="asterisk_input"></span></label>
            <div class="div-cal-icon">
                <input class="icon-field" id="departDate" name="D" readonly="" type="text" value="" sd_sf="ovmm">
            </div>
        </div>
        <div id="searchRangeCalendar" class="mbsc-comp"></div>
        <div style="clear:both; padding-top:15px;"></div>
            <div class="one-half-responsive third">
                <label for="show-bubble">Number of Guests <span class="asterisk_input"></span></label>
                <div class="div-user-icon">
                    <input id="show-bubble" value="2 Gu" class="icon-field" sd_sf="ovmm">
                    <input id="adult-label" type="hidden" value="Guests" sd_sf="ovmm">
                    <input id="child-label" type="hidden" value="Children" sd_sf="ovmm">
                    <input id="infant-label" type="hidden" value="Infant" sd_sf="ovmm">
                    <input id="show-child" type="hidden" value="0" sd_sf="ovmm">
                    <input id="show-infant" type="hidden" value="0" sd_sf="ovmm">
                </div>
            </div>
            <!--popup--><div id="demo-bubble" class="mbsc-comp" style="display: none;">
                <div class="mbsc-align-center mbsc-padding">
                    <div>
                        <label class="tooltip-rate" data-hasqtip="600" oldtitle=" Guests" title="">
                            Guests<span class="asterisk_input"></span>
                        </label>
                        <div class="numble-wrapper"><input type="number" maxlength="2" id="adult" class="text-input-one numble-adult numble-original" value="2" sd_sf="ovmm" style="display: none;"><div class="numble-control"><div class="numble-value" contenteditable="" sd_sf="ovmm">2</div><span class="numble-increment numble-arrow">▲</span><span class="numble-decrement numble-arrow">▼</span></div></div>
                    </div>

                </div>
            </div>
                <div class="one-half-responsive last">
                    <label for="Pc">Promotional/Group Code</label>
                    <input class="text-input-one" id="Pc" name="Pc" type="text" value="" sd_sf="ovmm">
                    <input type="hidden" id="mem-discount" value="" sd_sf="ovmm">
                </div>
<input id="showSiteOnly" name="showSiteOnly" type="hidden" value="no" sd_sf="ovmm"><input id="showDockOnly" name="showDockOnly" type="hidden" value="no" sd_sf="ovmm"><input id="siteValidation" name="siteValidation" type="hidden" value="no" sd_sf="ovmm">
        <div id="rv-fields" style="clear:both; display:none;">
                <div id="rvtype-field" class="one-third-responsive rv-placeholder div-down-icon-rv" style="display: none;">
                    <select id="T" name="T" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="T_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose type" sd_sf="ovmm">
                </div>
                <div id="rvlength-field" class="one-third-responsive rv-placeholder div-down-icon-rv" style="display: none;">
                    <select id="L" name="L" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="L_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose length" sd_sf="ovmm">
                </div>
                <div id="slide-field" class="one-third-responsive last rv-placeholder" style="display: none;">
                    <label>
                        Dwelling Slide
                    </label>
                    <div class="div-down-icon-rv">
                        <select id="S" name="S" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="S_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose slide" sd_sf="ovmm">
                    </div>
                </div>


        </div>

    <div id="boat-fields" style="clear:both; display:none;">
            <div class="one-forth-responsive">
                <label>Boat Type</label>
                <select id="Bt" name="Bt" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="Bt_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose type" sd_sf="ovmm">
            </div>
            <div class="one-forth-responsive">
                <label>Boat Draft</label>
                <select id="Bd" name="Bd" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="Bd_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose draft" sd_sf="ovmm">
            </div>
            <div class="one-forth-responsive">
                <label>Boat Length</label>
                <select id="Bl" name="Bl" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="Bl_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose length" sd_sf="ovmm">
            </div>
            <div class="one-forth-responsive">
                <label>Boat Width</label>
                <select id="Bw" name="Bw" class="dropdown mbsc-comp mbsc-sel-hdn" tabindex="-1"></select><input type="text" id="Bw_dummy" class="mbsc-select-input mbsc-control " readonly="" placeholder="Choose width" sd_sf="ovmm">
            </div>

    </div>

    <div class="clearfix"></div>




    <div id="notification" style="clear:both; padding-top:15px; display:none;">
        <div class="small-notification red-notification">
            <p class="center-text uppercase">
                <i class="fa fa-warning" aria-hidden="true"></i>
                <span id="notification-text"></span>
                <a href="#" class="close-small-notification">x</a>
            </p>
        </div>
    </div>
    <div id="notification-big" style="clear:both; padding-top:15px; display:none;">
        <div class="big-notification red-notification half-bottom">
            <h5 class="uppercase"><i class="fa fa-warning" aria-hidden="true"></i> Important</h5>
            <a href="#" class="close-big-notification">x</a>
            <p><span id="big-notification-text"></span></p>
        </div>
    </div>

    <div style="clear:both; padding-top:15px;">
            <div class="rate-link"><a class="button button-green center-button half-bottom" href="/Rates/Index/2633/3" id="booking-rate" style="font-size:15px; width:230px; background-color:#287a51; color:#ffffff;">Search Availability</a></div>
            <div class="map-links" style="display:none;"><a class="button button-green center-button half-bottom" href="/Map/Index/2633/3" id="booking-map" style="font-size:15px; width:230px; background-color:#287a51; color:#ffffff;">Search Availability</a></div>

    </div>
</div>



<input data-val="true" data-val-required="The CountryRunningFrom field is required." id="CountryRunningFrom" name="CountryRunningFrom" type="hidden" value="Australia" sd_sf="qjj-27">
<input id="Dn" name="Dn" type="hidden" value="20260129024123" sd_sf="ovmm">
<input data-val="true" data-val-required="The TA field is required." id="TA" name="TA" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The G field is required." id="G" name="G" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The Ar field is required." id="Ar" name="Ar" type="hidden" value="0" sd_sf="ovmm">
<input id="Mp" name="Mp" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The Ci field is required." id="Ci" name="Ci" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The RVTypeMandatory field is required." id="RVTypeMandatory" name="RVTypeMandatory" type="hidden" value="False" sd_sf="ovmm">
<input data-val="true" data-val-required="The RVLengthMandatory field is required." id="RVLengthMandatory" name="RVLengthMandatory" type="hidden" value="False" sd_sf="ovmm">
<input data-val="true" data-val-required="The AutoExpandAdditionals field is required." id="AutoExpandAdditionals" name="AutoExpandAdditionals" type="hidden" value="False" sd_sf="ovmm">
<input data-val="true" data-val-required="The ClientIdChanged field is required." id="ClientIdChanged" name="ClientIdChanged" type="hidden" value="False" sd_sf="ovmm">
<input data-val="true" data-val-required="The UseAvailabilityCalendar field is required." id="UseAvailabilityCalendar" name="UseAvailabilityCalendar" type="hidden" value="True" sd_sf="ovmm">
<input id="AvailCalendarJSHeader" name="AvailCalendarJSHeader" type="hidden" value="" sd_sf="ovmm">
<input data-val="true" data-val-required="The DefaultAdults field is required." id="DefaultAdults" name="DefaultAdults" type="hidden" value="2" sd_sf="ovmm">
<input data-val="true" data-val-required="The AllowGroupBookings field is required." id="AllowGroupBookings" name="AllowGroupBookings" type="hidden" value="True" sd_sf="ovmm">
<input id="IPAddress" name="IPAddress" type="hidden" value="185.99.220.148" sd_sf="qjj-4">
<input id="isMarina" name="isMarina" type="hidden" value="False" sd_sf="ovmm">
<input id="show-member" name="show-member" type="hidden" value="False" sd_sf="ovmm">
<input id="prop-placeholder" name="prop-placeholder" type="hidden" value="All Properties" sd_sf="ovmm">
<input data-val="true" data-val-required="The V field is required." id="V" name="V" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The Z field is required." id="Z" name="Z" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The Y field is required." id="Y" name="Y" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The X field is required." id="X" name="X" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The M field is required." id="M" name="M" type="hidden" value="0" sd_sf="ovmm">
<input id="P" name="P" type="hidden" value="Currawong Beach Cottages" sd_sf="ovmm">
<input data-val="true" data-val-required="The Int32 field is required." id="Av" name="Av" type="hidden" value="0" sd_sf="ovmm">
<input data-val="true" data-val-required="The Int32 field is required." id="Rv" name="Rv" type="hidden" value="0" sd_sf="ovmm">
<input id="Li" name="Li" type="hidden" value="0" sd_sf="ovmm">
<input id="RateView" name="RateView" type="hidden" value="#!/rooms" sd_sf="ovmm">
<input id="lblType" name="lblType" type="hidden" value="Choose type" sd_sf="ovmm">
<input id="lblLength" name="lblLength" type="hidden" value="Choose length" sd_sf="ovmm">
<input id="lblSlide" name="lblSlide" type="hidden" value="Choose slide" sd_sf="ovmm">
<input id="lblArea" name="lblArea" type="hidden" value="Cottages" sd_sf="ovmm">
<input id="lblCat" name="lblCat" type="hidden" value="Category" sd_sf="ovmm">
<input id="lblGuest" name="lblGuest" type="hidden" value="Guest" sd_sf="ovmm">
<input id="IdleURL" name="IdleURL" type="hidden" value="/Search/Idle/2633/3" sd_sf="ovmm">
<input id="ArrivalText" name="ArrivalText" type="hidden" value="Arrival" sd_sf="ovmm">
<input id="DepartureText" name="DepartureText" type="hidden" value="Departure" sd_sf="ovmm">
<input data-val="true" data-val-required="The Boolean field is required." id="UseTraining" name="UseTraining" type="hidden" value="False" sd_sf="ovmm">
<input id="ChooseProperty" name="ChooseProperty" type="hidden" value="Choose Property" sd_sf="ovmm">
<input id="BubbleCancelText" name="BubbleCancelText" type="hidden" value="Cancel" sd_sf="ovmm">
<input id="MustFieldsText" name="MustFieldsText" type="hidden" value="You must enter the following fields" sd_sf="ovmm">
<input id="VRTypeText" name="VRTypeText" type="hidden" value="Dwelling Type" sd_sf="ovmm">
<input id="VRLengthText" name="VRLengthText" type="hidden" value="Dwelling Length" sd_sf="ovmm">
<input id="PropertyText" name="PropertyText" type="hidden" value="property" sd_sf="ovmm">
<input id="DatesText" name="DatesText" type="hidden" value="dates" sd_sf="ovmm">
<input id="TypeOfReservationText" name="TypeOfReservationText" type="hidden" value="type of reservation" sd_sf="ovmm">


<input id="Bc" name="Bc" type="hidden" value="#287a51" sd_sf="ovmm"><input id="Bf" name="Bf" type="hidden" value="#ffffff" sd_sf="ovmm"><input id="lblDwelling" name="lblDwelling" type="hidden" value="Dwelling" sd_sf="ovmm"><input data-val="true" data-val-required="The Int32 field is required." id="lstOfPropIdsForPhotos_0_" name="lstOfPropIdsForPhotos[0]" type="hidden" value="1" sd_sf="ovmm"><input id="ratesError" name="ratesError" type="hidden" value="0" sd_sf="ovmm">




                </div>
