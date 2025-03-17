-- 1 hs chi giam tru 1 lan

select sum(tien_giam) from ttkd_bsc.ct_giamtru where thang = 202501 --96915000
;
delete from ttkd_bsc.ct_giamtru where thang=202501
;
insert into ttkd_bsc.ct_giamtru (ma_gd, thang, tien_giam, nguon_giam, ly_do, ma_nv, ma_to, ma_pb)
		    select ma_gd, thang_tldg_dt thang, 15000 tien_giam, 'Luong don gia' nguon_giam
						 , 'Don gia thu ho so tai nha theo VB 746/TTr-NS 31/12/2019', manv_ptm, ma_to, ma_pb    
			   from ttkd_bsc.ct_bsc_ptm a
			   where kieuhd_id = 2 and thang_tldg_dt = 202501 
						 and not exists (select 1 from ttkd_bsc.ct_giamtru where ma_gd=a.ma_gd) -- de xet 1 HS (nhieu thue bao) chi bi giam tien 1 lan 
			   group by ma_gd, manv_ptm, ma_to, ma_pb, thang_tldg_dt
			   ;
commit; 

		select kieuhd_id from css_hcm.hd_khachhang;
rollback;

create index ttkd_bsc.idx_mnv_giamtru on ttkd_bsc.ct_giamtru (ma_nv);

