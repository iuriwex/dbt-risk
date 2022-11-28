--Creates a nice acd dataset to use for final dataset
with pro_sandbox.ca_nice_acd as (
    SELECT * FROM {{('elt_tool__ca_nice_acd')}}
)
select distinct 
          cast(a.contact_id as varchar) as nice_contact_id 
        ,NULLIF(a.acw__time,'')acw__time
        ,NULLIF(a.abandon__time,'')abandon__time
        ,NULLIF(a.active__talk__time,'')active__talk__time
        ,NULLIF(a.agent_id,'')agent_id
        ,NULLIF(a.agent__name,'')agent__name
        ,NULLIF(a.available__time,'')available__time
        ,NULLIF(a.callback__time,'')callback__time
        ,NULLIF(a.concurrent__time,'')concurrent__time
        ,NULLIF(a.consult__time,'')consult__time
        ,NULLIF(a.contact__duration,'')contact__duration
        ,NULLIF(a.contact__start__date__time,'')contact__start__date__time
        ,NULLIF(a.contact__end__date__time,'')contact__end__date__time
        ,NULLIF(a.contact__time,'')contact__time
        ,NULLIF(a.contact__type,'')contact__type
        ,NULLIF(a.direction,'')direction
        ,NULLIF(a.handle__time,'')handle__time
        ,NULLIF(a.hold__time,'')hold__time
        ,NULLIF(a.ivr__time,'')ivr__time
        ,NULLIF(a.inbound_aht,'')inbound_aht
        ,NULLIF(a.inqueue__time,'')inqueue__time
        ,NULLIF(a.outbound_aht,'')outbound_aht
        ,NULLIF(a.parked__time,'')parked__time
        ,NULLIF(a.refused__time,'')refused__time
        ,NULLIF(a.speed__of__answer,'')speed__of__answer
        ,NULLIF(a.team__name,'')team__name
        ,NULLIF(a.unavailable__time,'')unavailable__time
        ,NULLIF(a.wait__time,'')wait__time
        ,NULLIF(a.working__time,'')working__time
from nice_acd_rss.wex__aht_reportdownload   a
where a.active__agent='True';