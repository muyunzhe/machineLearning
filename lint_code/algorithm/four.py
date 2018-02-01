#!/usr/bin/python
# -*- encoding:utf-8 -*-

def find_min(nums):
    if not nums:
        return False
    length = len(nums)
    left, right = 0, length - 1
    while nums[left] >= nums[right]:
        if right - left == 1:
            return nums[right]
        mid = (left + right) / 2
        if nums[left] == nums[mid] == nums[right]:
            return min(nums)
        if nums[left] <= nums[mid]:
            left = mid
        if nums[right] >= nums[mid]:
            right = mid
    return nums[0]


def power(base, exponent):
    if equal_zero(base) and exponent < 0:
        raise ZeroDivisionError
    ret = power_value(base, base(exponent))
    if exponent < 0 :
        return 1.0 / ret
    else:
        return ret

def equal_zero(num):
    if abs(num - 0.0) < 0.0000001:
        return True

def power_value(base, exponent):
    if exponent == 0:
        return 1
    if exponent == 1:
        return base
    ret = power_value(base, exponent >> 1)
    ret *= ret
    if exponent & 1 == 1:
        ret *= base
    return ret

