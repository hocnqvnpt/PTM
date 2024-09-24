--select * from ttkdhcm_ktnv.db_act_593;
    
update ttkd_bsc.ct_bsc_ptm a
    set (tien_dnhm, tien_tt, soseri, ngay_tt)=
            (select round(sotien/1.1,0), round(sotien/1.1,0), so_hd, ngay_thu
                from ttkdhcm_ktnv.db_act_593
                where to_number(to_char(ngay_imp,'yyyymm'))>=202404 
                        and sotien>0 and lower(nhomphi) like 'ph_ h_a m_ng%' and username=a.ma_tb)
    -- select tien_dnhm, tien_tt, soseri, ngay_tt, manv_ptm from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm>202312 and chuquan_id=264 and loaihd_id=1 and manv_ptm is not null 
                and exists (select 1 from ttkdhcm_ktnv.db_act_593
                                     where to_number(to_char(ngay_imp,'yyyymm'))>=202404 
                                        and sotien>0 and lower(nhomphi) like 'ph_ h_a m_ng%' and username=a.ma_tb);    


              
update ct_bsc_ptm a set (ghi_chu, xacnhan_khkt, thang_xacnhan_khkt)=
        (select max(nd_thu), sum(sotien), 202404 
            from ttkdhcm_ktnv.db_act_593
            where to_number(to_char(ngay_imp,'yyyymm'))>=202404 and (lower(nhomphi) like '%tr_ tr__c%' or lower(nhomphi) like '%c__c ph_t sinh%') 
                        and trangthai like '__ thu' and username=a.ma_tb)
    -- select thang_ptm, ma_tb, soseri, tien_tt, ngay_tt, tien_dnhm, thang_tldg_dnhm, thang_tldg_dt, thang_tlkpi, xacnhan_khkt, thang_xacnhan_khkt, ghi_chu from ct_bsc_ptm a
    where thang_ptm>=202312 and chuquan_id=264 and xacnhan_khkt is null and thang_tldg_dt is null --and ma_tb='fvn_ht779'
            and exists (select 1 from ttkdhcm_ktnv.db_act_593 
                                  where to_number(to_char(ngay_imp,'yyyymm'))>=202404 
                                               and (lower(nhomphi) like '%tr_ tr__c%' or lower(nhomphi) like '%c__c ph_t sinh%') and trangthai like '__ thu' and username=a.ma_tb);


commit;   


-- Kiem tra
select thang_ptm, ma_tb, soseri, tien_tt, ngay_tt, tien_dnhm, thang_tldg_dnhm, thang_tldg_dt, thang_tlkpi, xacnhan_khkt, thang_xacnhan_khkt, ghi_chu
    from ct_bsc_ptm a
    where thang_ptm>=202312 and manv_ptm is not null
        and chuquan_id=264 and thang_xacnhan_khkt=202404
        and exists (select * from ttkdhcm_ktnv.db_act_593 
                                where to_char(ngay_imp,'yyyymm')>='202404' and lower(nhomphi) like '%tr_ tr__c%' and username=a.ma_tb)
        and sothang_dc is null;
