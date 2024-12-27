---bang chi tien vb 344
select *  from vietanhvh.one_line_202409 where LYDO_KHONGTINH is not null and TIEN_THULAO_GOI > 0 and MANV_GOI is not null;
select ma_tb from vietanhvh.one_line_202409 where LYDO_KHONGTINH is not null and TIEN_THULAO_DNHM > 0 and MANV_PTM is not null;

select *  from vietanhvh.one_line_202410 where LYDO_KHONGTINH is not null and TIEN_THULAO_GOI > 0 and MANV_GOI is not null;
select * from vietanhvh.one_line_202410 where LYDO_KHONGTINH is not null and TIEN_THULAO_DNHM > 0 and MANV_PTM is not null;

select *  from vietanhvh.one_line_202411 where LYDO_KHONGTINH is not null and TIEN_THULAO_GOI > 0 and MANV_GOI is not null;
select * from vietanhvh.one_line_202411 where LYDO_KHONGTINH is not null and TIEN_THULAO_DNHM > 0 and MANV_PTM is not null;

----bang chi tien HHBG
select * from manpn.thulao_hhbg_dachi@ttkddbbk2 where thang in (202409,202410,202411);
----bang tonghop BSC MAN
select * from manpn.manpn_goi_tonghop_202411
where ma_tb = '84913808953';




---kiem tra chi tiên PTTT, nguyen tac không chi
select *  from vietanhvh.one_line_202409 where MAPB_PTM = 'VNP0700800' and TIEN_THULAO_DNHM > 0;
select sum(TIEN_THULAO_GOI)  from vietanhvh.one_line_202409 where MAPB_GOI = 'VNP0700800' and TIEN_THULAO_GOI > 0;

select *  from vietanhvh.one_line_202410 where MAPB_PTM = 'VNP0700800' and TIEN_THULAO_DNHM > 0;
select sum(TIEN_THULAO_GOI)  from vietanhvh.one_line_202410 where MAPB_GOI = 'VNP0700800' and TIEN_THULAO_GOI > 0;

select *  from vietanhvh.one_line_202411 where MAPB_PTM = 'VNP0700800' and TIEN_THULAO_DNHM > 0;
select sum(TIEN_THULAO_GOI)  from vietanhvh.one_line_202411 where MAPB_GOI = 'VNP0700800' and TIEN_THULAO_GOI > 0;
----END PTTT
----KIEM TRA CHI TRUNG hhbg của MAN
select *  from vietanhvh.one_line_202411 where LYDO_KHONGTINH is not null and TIEN_THULAO_GOI > 0 and HESO_HHBG > 0 and MANV_GOI is not null and ma_tb in (select SO_THUE_BAO from manpn.thulao_hhbg_dachi@ttkddbbk2 where thang in (202409,202410,202411));
select *  from ttkd_bsc.ct_bsc_ptm where loaitb_id = 21 and thang_ptm = 202408 and thang_tldg_dt = 202408 and LUONG_DONGIA_NVPTM > 0 and TIEN_THULAO_GOI > 0 and MA_NVptm is not null and ma_tb in (select SO_THUE_BAO from manpn.thulao_hhbg_dachi@ttkddbbk2 where thang in (202408, 202409,202410));

-----KIEM TRA chi TRUNG Kenh XHHH
		--===== KHÔNG VÍ
				---kiem tra tren dthu DNHM
				Select THANG, MA_DONHANG, SODT_KH, HOAHONG
				From khanhtdt_ttkd.CTVXHH_CHOTT_KHONGVI_2024
				;
				Select THANG, MA_DONHANG, SODT_KH, SUM(HOAHONG)
				From khanhtdt_ttkd.CTVXHH_CHOTT_KHONGVI_2024
				Where thang >= 202410 and sodt_kh in
							    (    select ma_tb from vietanhvh.one_line_202411 where TIEN_THULAO_DNHM > 0
							    )
				Group by THANG, MA_DONHANG, SODT_KH Order by thang
				;
				---kiem tra tren dthu mua goi
				Select THANG, MA_DONHANG, SODT_KH, SUM(HOAHONG)
				From khanhtdt_ttkd.CTVXHH_CHOTT_KHONGVI_2024
				Where thang >= 202410 and sodt_kh in
				    (   
							 select ma_tb from vietanhvh.one_line_202411 where TIEN_THULAO_GOI > 0 and HESO_HHBG > 0
						
				    )
				Group by THANG, MA_DONHANG, SODT_KH Order by thang;
				
				--===== CO VI
				Select THANG, MA_DONHANG, SO_TB, SUM(HOAHONG_1)
				From khanhtdt_ttkd.SMCS_COVI_2024
				Where thang >= 202410 and SO_TB in
				    (   select ma_tb from vietanhvh.one_line_202411 where TIEN_THULAO_DNHM > 0							
				    )
				Group by THANG, MA_DONHANG, SO_TB Order by thang
				;
				---kiem tra tren dthu mua goi
				Select THANG, MA_DONHANG, SO_TB, SUM(HOAHONG_1)
				From khanhtdt_ttkd.SMCS_COVI_2024
				Where thang >= 202410 and SO_TB in
				    (   
							 select ma_tb from vietanhvh.one_line_202411 where TIEN_THULAO_GOI > 0 and HESO_HHBG > 0
				    )
				Group by THANG, MA_DONHANG, SO_TB Order by thang
				;
	---END KENH CTV-XHH