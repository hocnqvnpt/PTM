select * from ttkd_bsc.nhanvien where ma_pb = 'VNP0703000' and thang in (202502);

insert into ttkd_bsc.blkpi_danhmuc_kpi ;
select MA_KPI, TEN_KPI, LOAI, NGUOI_XULY, THANG_BD, THANG_KT, NGAY_INS, GHICHU, DONVI, 202502 THANG, MANV_LEAD, GIAO, THUCHIEN, TYLE_THUCHIEN, MUCDO_HOANTHANH, DIEM_CONG, DIEM_TRU, CHITIEU_GIAO, TYTRONG
from ttkd_bsc.blkpi_danhmuc_kpi a
		
where a.thang = 202502
			and ma_kpi in ('VNP-HNHCM_KDOL_24.1')
			;
----Update ty trong
	update 

commit;

select *
from ttkd_bsc.blkpi_danhmuc_kpi a
where thang = 202502 and nguoi_xuly = 'BHOL tự nhập'
;

----Update chitieu_giao
	update ttkd_bsc.bangluong_kpi a set chitieu_giao = 100
--	select * from ttkd_bsc.bangluong_kpi a
	where a.thang = 202502 and a.ma_pb = 'VNP0703000'
					and exists (select *
												from ttkd_bsc.blkpi_danhmuc_kpi
												where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and CHITIEU_GIAO = 1 and ma_kpi = a.ma_kpi )
	;
rollback;
commit;


Select * From ttkd_bsc.bangluong_kpi Where thang = 202502 and MA_KPI = 'HCM_SL_CSKHH_004' and ma_nv in ('VNP016962', 'VNP017680') Order by ma_pb, ma_vtcv;

 insert into ttkd_bsc.blkpi_danhmuc_kpi_vtcv;
 select * From ttkd_bsc.blkpi_danhmuc_kpi_vtcv Where thang = 202502 and MA_VTCV in ('VNP-HNHCM_KDOL_23');
commit;
rollback;


select * From ttkd_bsc.bangluong_kpi Where thang = 202502 and ma_nv = 'CTV072956';

;

update ttkd_bsc.bangluong_kpi_bhol set GIAO = round(giao, 3) Where thang = 202502; and ma_nv in ('VNP017740') and ma_kpi = 'HCM_DT_VNPTT_011';
select * From ttkd_bsc.bangluong_kpi Where thang = 202502 and ma_nv in ('VNP017740') and ma_kpi = 'HCM_DT_VNPTT_011';

select THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG, TYTRONG, DONVI_TINH, CHITIEU_GIAO,  GIAO
From ttkd_bsc.bangluong_kpi 
Where thang = 202502 and ma_pb in ('VNP0703000')
order by ma_kpi, ma_to, ma_nv
;
delete from ttkd_bsc.bangluong_kpi_bhol where thang = 202502;
select * from  ttkd_bsc.bangluong_kpi_bhol where thang = 202502;
----Check tytrong
select ma_nv, ten_nv, ten_pb, sum(tytrong) tytrong from  ttkd_bsc.bangluong_kpi where thang = 202502-- and ma_pb in ('VNP0703000')
group by ma_nv, ten_nv, ten_pb
having sum(tytrong)> 100
;
		---Check ds BHOL
		select ma_nv, sum(tytrong) tytrong from  ttkd_bsc.bangluong_kpi_bhol where thang = 202502
		group by ma_nv
		;
----Check lech bangluongkpi
select ma_nv, ma_kpi, tytrong From ttkd_bsc.bangluong_kpi Where thang = 202502 and ma_pb in ('VNP0703000')
minus
select ma_nv, ma_kpi, tytrong from  ttkd_bsc.bangluong_kpi_bhol where thang = 202502 and ma_pb in ('VNP0703000')
;
	---chenh lech bhol
		select ma_nv, ma_kpi, tytrong From ttkd_bsc.bangluong_kpi_bhol Where thang = 202502 and ma_pb in ('VNP0703000') --and ghichu != 'BSC không giao'
		minus
		select ma_nv, ma_kpi, tytrong from ttkd_bsc.bangluong_kpi where thang = 202502 and ma_pb in ('VNP0703000')
		;
----update TYTRONG----
MERGE INTO ttkd_bsc.bangluong_kpi a
USING ttkd_bsc.bangluong_kpi_bhol b
ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
WHEN MATCHED THEN
		UPDATE SET a.tytrong = b.tytrong
--			select * from ttkd_bsc.bangluong_kpi a
WHERE thang = 202502
			and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
													from (select ma_nv, ma_kpi, tytrong From ttkd_bsc.bangluong_kpi_bhol Where thang = 202502 and ma_pb in ('VNP0703000')
																		minus
																		select ma_nv, ma_kpi, tytrong from ttkd_bsc.bangluong_kpi where thang = 202502 and ma_pb in ('VNP0703000'))
													)
													;
			and not exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and a.ma_kpi = ma_kpi)
													
;
----update CHITIEU_GIAO----
		MERGE INTO ttkd_bsc.bangluong_kpi a
		USING ttkd_bsc.bangluong_kpi_bhol b
		ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
		WHEN MATCHED THEN
				UPDATE SET a.chitieu_giao = b.chitieu_giao
		--			select * from ttkd_bsc.bangluong_kpi a
		WHERE a.thang = 202502 and ma_pb in ('VNP0703000')
					and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
															from (select ma_nv, ma_kpi, chitieu_giao From ttkd_bsc.bangluong_kpi_bhol Where thang = a.thang and ma_pb in ('VNP0703000')
																				minus
																				select ma_nv, ma_kpi, chitieu_giao from ttkd_bsc.bangluong_kpi where thang = a.thang and ma_pb in ('VNP0703000'))
															)
															;
		and exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and a.ma_kpi = ma_kpi)
;
----update GIAO----
		MERGE INTO ttkd_bsc.bangluong_kpi a
		USING ttkd_bsc.bangluong_kpi_bhol b
		ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
		WHEN MATCHED THEN
				UPDATE SET a.giao = round(b.giao, 3)
		--			select * from ttkd_bsc.bangluong_kpi a
		WHERE a.thang = 202502 and ma_pb in ('VNP0703000')
					and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
															from (select ma_nv, ma_kpi, giao From ttkd_bsc.bangluong_kpi_bhol Where thang = a.thang and ma_pb in ('VNP0703000')
																				minus
																				select ma_nv, ma_kpi, giao from ttkd_bsc.bangluong_kpi where thang = a.thang and ma_pb in ('VNP0703000'))
															)
															;
		and exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and a.ma_kpi = ma_kpi)
;
----update DONVI_TINH----
		MERGE INTO ttkd_bsc.bangluong_kpi a
		USING ttkd_bsc.bangluong_kpi_bhol b
		ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
		WHEN MATCHED THEN
				UPDATE SET a.donvi_tinh = b.donvi_tinh
		--			select * from ttkd_bsc.bangluong_kpi a
		WHERE a.thang = 202502 and ma_pb in ('VNP0703000')
					and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
															from (select ma_nv, ma_kpi, donvi_tinh From ttkd_bsc.bangluong_kpi_bhol Where thang = a.thang and ma_pb in ('VNP0703000')
																				minus
																				select ma_nv, ma_kpi, donvi_tinh from ttkd_bsc.bangluong_kpi where thang = a.thang and ma_pb in ('VNP0703000'))
															)
															;
		and exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and a.ma_kpi = ma_kpi)
;
----update MUCDO_HOANTHANG----
		MERGE INTO ttkd_bsc.bangluong_kpi a
		USING ttkd_bsc.bangluong_kpi_bhol b
		ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
		WHEN MATCHED THEN
				UPDATE SET a.MUCDO_HOANTHANH = b.MUCDO_HOANTHANH
		--			select * from ttkd_bsc.bangluong_kpi a
		WHERE a.thang = 202502 and ma_pb in ('VNP0703000')
					and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
															from (select ma_nv, ma_kpi, MUCDO_HOANTHANH From ttkd_bsc.bangluong_kpi_bhol Where thang = a.thang and ma_pb in ('VNP0703000')
																				minus
																				select ma_nv, ma_kpi, MUCDO_HOANTHANH from ttkd_bsc.bangluong_kpi where thang = a.thang and ma_pb in ('VNP0703000'))
															)
															
		and exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and MUCDO_HOANTHANH = 1 and a.ma_kpi = ma_kpi)
;
----update THUCHIEN----
		MERGE INTO ttkd_bsc.bangluong_kpi a
		USING ttkd_bsc.bangluong_kpi_bhol b
		ON (a.thang = b.thang and a.ma_nv = b.ma_nv and a.ma_kpi = b.ma_kpi)
		WHEN MATCHED THEN
				UPDATE SET a.THUCHIEN = b.THUCHIEN
		--			select * from ttkd_bsc.bangluong_kpi a
		WHERE a.thang = 202502 and ma_pb in ('VNP0703000')
					and (ma_nv, ma_kpi) in (select ma_nv, ma_kpi 
															from (select ma_nv, ma_kpi, THUCHIEN From ttkd_bsc.bangluong_kpi_bhol Where thang = a.thang and ma_pb in ('VNP0703000')
																				minus
																				select ma_nv, ma_kpi, THUCHIEN from ttkd_bsc.bangluong_kpi where thang = a.thang and ma_pb in ('VNP0703000'))
															)
															
		and exists (select *
							from ttkd_bsc.blkpi_danhmuc_kpi
							where thang = 202502 and nguoi_xuly = 'BHOL tự nhập' and THUCHIEN = 1 and a.ma_kpi = ma_kpi)
;
----Khi can thiet INSERT them khi 1 KPI xác định từ mẫu KPI người khác---
	INSERT INTO ttkd_bsc.bangluong_kpi (THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG, TYTRONG, DONVI_TINH, CHITIEU_GIAO)
	
	select a.THANG, b.MA_KPI, b.TEN_KPI, a.MA_NV, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_TO, a.TEN_TO, a.MA_PB, a.TEN_PB, NGAYCONG, TYTRONG, DONVI_TINH, CHITIEU_GIAO
	from ttkd_bsc.nhanvien a
				join ttkd_bsc.bangluong_kpi b on a.thang = b.thang 
	where a.thang = 202502 --and ma_kpi = 'HCM_CL_CVIEC_029' 
					and a.ma_nv in ('CTV089303') ----Nhân viên cần insert
					and b.ma_nv = 'VNP019958'  --nhan viên cung vị trí Mẫu
	;
commit;
rollback;

	----Khi can thiet INSERT them khi 1 bô KPI của 1 vị trí cv---
	INSERT INTO ttkd_bsc.bangluong_kpi (THANG, MA_KPI, TEN_KPI, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, NGAYCONG, TYTRONG, DONVI_TINH, CHITIEU_GIAO)
	;
	select a.THANG, b.MA_KPI, c.TEN_KPI, a.MA_NV, a.TEN_NV, a.MA_VTCV, a.TEN_VTCV, a.MA_TO, a.TEN_TO, a.MA_PB, a.TEN_PB, 20 NGAYCONG
	from ttkd_bsc.nhanvien a
				join ttkd_bsc.blkpi_danhmuc_kpi_vtcv b on a.thang = b.thang and a.ma_vtcv = b.ma_vtcv
				join ttkd_bsc.blkpi_danhmuc_kpi c on a.thang = c.thang and b.ma_kpi = c.ma_kpi
	where a.thang = 202502  
					and a.ma_nv in (select * from ttkd_bsc.x_nhanvien_202502_moi)
	;
commit;
rollback;