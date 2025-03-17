with dg as (select 202412 thang, a.thang_ptm, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu, count(*) sl_tbo, sum(luong_dongia_nvptm) luong_dongia, 'nvptm' nguon
					from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on b.thang = a.thang_ptm and b.donvi = 'VTTP' and a.manv_ptm = b.ma_nv
					where a.thang_ptm >= 202409
								and loaitb_id in (58, 59, 61, 171, 20)
								and nvl(thang_tldg_dt, 999999) >= 202412
					group by a.thang_ptm, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu
					union all
					select 202412 thang, a.thang_ptm, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu, 0 sl_tbo, sum(luong_dongia_dnhm_nvptm * heso_hotro_nvptm) luong_dongia, 'dnhm' nguon
					from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on b.thang = a.thang_ptm and b.donvi = 'VTTP' and a.manv_ptm = b.ma_nv
					where a.thang_ptm >= 202409
								and loaitb_id in (58, 59, 61, 171, 20)
								and nvl(thang_tldg_dnhm, 999999) >= 202412
					group by a.thang_ptm, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu
					union all
					select 202412 thang, a.thang_ptm, b.ma_nv, b.ten_nv, b.ten_to, b.ten_pb, dich_vu, 0 sl_tbo, sum(luong_dongia_dnhm_nvptm * heso_hotro_nvhotro) tong_luong_dg_dnhm_nvptm, 'dnhm' nguon
					from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on b.thang = a.thang_ptm and b.donvi = 'VTTP' and a.manv_hotro = b.ma_nv
					where a.thang_ptm >= 202409
								and loaitb_id in (58, 59, 61, 171, 20)
								and nvl(thang_tldg_dnhm, 999999) >= 202412
					group by a.thang_ptm, b.ma_nv, b.ten_nv, b.ten_to, b.ten_pb, dich_vu
					)
		, dg1 as			
				(select a.thang, a.thang_ptm, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu, sl_tbo, a.luong_dongia, nvl(x.heso_dthu, 0.8) heso_dthu
							--, sum(luong_dongia) luong_dongia
				from dg a
								left join ttkd_bsc.bang_heso_dthu x on a.thang_ptm = x.thang and x.donvi = 'VTTP' and a.manv_ptm = x.ma_nv
				)
select a.thang, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu, sum(sl_tbo) sl_tbo, round(sum(a.luong_dongia * heso_dthu), 0) luong_dongia_wish
from dg1 a where a.thang_ptm >= 202410
group by a.thang, manv_ptm, tennv_ptm, a.ten_to, a.ten_pb, dich_vu
;

MERGE INTO x
USING
			(with hs as (
								select a.thang_ptm, manv_ptm, a.ten_pb, sum(DOANHTHU_KPI_DNHM) + sum(DOANHTHU_KPI_NVPTM) tong_dthu_kpi
								from ttkd_bsc.ct_bsc_ptm a
											join ttkd_bsc.nhanvien b on b.thang = 202412 and b.donvi = 'VTTP' and a.manv_ptm = b.ma_nv
								where a.thang_ptm = 202412
											and loaitb_id in (58, 59, 61, 171)
								group by a.thang_ptm, manv_ptm, a.ten_pb
								)
				select hs.*, case when tong_dthu_kpi < 1800000 then 0.8
									when tong_dthu_kpi >= 1800000 then 1
									else 0 end heso_dthu
				from hs) y
ON (x.ma_nv = y.manv_ptm)
WHEN MATCHED THEN
	UPDATE SET
WHERE
;


select * from ttkd_bsc.bang_heso_dthu;
select sum(TIEN_THULAO), count(*) from ttkd_bsc.tonghop_ct_dongia_ptm where thang = 202411 and donvi = 'VTTP'; and loaitb_id = 21